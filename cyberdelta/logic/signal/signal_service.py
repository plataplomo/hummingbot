"""Signal service for validation and routing of trading signals.

This module provides the SignalService class that validates trading signals
and routes them to risk assessment using validated AppSettings configuration.
"""

from __future__ import annotations

from typing import List, Optional

from cyberdelta.config.structlog_config import get_logger

from cyberdelta.application.event_bus import EventBus
from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import TradeSignal

logger = get_logger(__name__)


class SignalService:
    """Signal validation and routing service using validated AppSettings.
    
    This service handles:
    - Trading signal validation against configured rules
    - Signal routing to risk assessment
    - Signal quality checks and filtering
    - Event publishing for validated signals
    
    Configuration Structure (config.risk.checkers):
    - enable_symbol_validation: Whether to validate symbol format
    - enable_price_sanity: Whether to check price ranges
    - enable_profitability: Whether to check profit expectations
    - thresholds: ValidationThresholds
      - min_signal_confidence: Minimum confidence level required
      - min_price: Minimum allowed price
      - max_price: Maximum allowed price
      - min_profitability: Minimum expected profit
    
    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL configuration from AppSettings, NO hardcoded values
    - Uses Symbol objects, NOT strings
    - Uses ExchangeName enum, NOT strings
    - NO assumptions about signal validity
    - Fail fast on validation errors
    """
    
    def __init__(
        self,
        config: AppSettings,
        event_bus: EventBus,
    ):
        """Initialize signal service with configuration and dependencies.
        
        Args:
            config: Application settings containing all configuration
            event_bus: Event bus for publishing validated signals
        """
        self.config = config
        self._event_bus = event_bus
        
        # Extract checker configuration - NO hardcoded defaults
        self._checker_config = config.risk.checkers
        self._thresholds = self._checker_config.thresholds
        
        # Cache frequently accessed validation settings
        self._enable_symbol_validation = self._checker_config.enable_symbol_validation
        self._enable_price_sanity = self._checker_config.enable_price_sanity
        self._enable_profitability = self._checker_config.enable_profitability
        self._enable_confidence_check = self._checker_config.enable_confidence_check
        
        # Extract validation thresholds
        self._min_confidence = self._thresholds.min_signal_confidence
        self._min_price = self._thresholds.min_price
        self._max_price = self._thresholds.max_price
        self._min_profitability = self._thresholds.min_profitability
        
        logger.info(
            "signal_service_initialized",
            symbol_validation_enabled=self._enable_symbol_validation,
            price_sanity_enabled=self._enable_price_sanity,
            profitability_enabled=self._enable_profitability,
            confidence_check_enabled=self._enable_confidence_check,
            min_confidence=float(self._min_confidence),
            min_price=float(self._min_price),
            max_price=float(self._max_price)
        )
    
    async def process_signal(self, signal: TradeSignal) -> bool:
        """Process and validate a trading signal.
        
        Args:
            signal: Trading signal to process and validate
            
        Returns:
            True if signal was validated and forwarded, False if rejected
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - ALL validation rules from config
        - Uses Symbol/ExchangeName types for validation
        - Explicit validation violations logged
        - NO silent filtering or defaults
        """
        logger.debug(
            "signal_processing_started",
            signal_id=signal.signal_id,
            symbol=signal.symbol.value,
            exchange=signal.exchange.value if hasattr(signal.exchange, 'value') else str(signal.exchange),
            side=signal.side.value if hasattr(signal.side, 'value') else str(signal.side)
        )
        
        try:
            # Validate signal against configured rules
            violations = await self.validate_signal(signal)
            
            if violations:
                # Signal failed validation
                logger.warning(
                    "signal_validation_failed",
                    signal_id=signal.signal_id,
                    violation_count=len(violations),
                    violations=violations
                )
                return False
            
            # Signal passed validation - publish for risk assessment
            await self._event_bus.publish("trading_signal", signal)
            
            logger.info(
                "signal_validated_and_published",
                signal_id=signal.signal_id,
                symbol=signal.symbol.value,
                exchange=signal.exchange.value if hasattr(signal.exchange, 'value') else str(signal.exchange),
                side=signal.side.value if hasattr(signal.side, 'value') else str(signal.side),
                price=float(signal.price) if signal.price else None
            )
            
            return True
            
        except Exception as e:
            logger.error(
                "signal_processing_error",
                signal_id=signal.signal_id,
                error=str(e),
                exc_info=True
            )
            return False
    
    async def validate_signal(self, signal: TradeSignal) -> List[str]:
        """Validate signal against configured validation rules.
        
        Args:
            signal: Trading signal to validate
            
        Returns:
            List of validation violations (empty if valid)
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - ALL validation rules from config.risk.checkers
        - Uses typed checking (Symbol, ExchangeName)
        - Explicit violation messages with context
        - NO assumptions about signal structure
        """
        violations = []
        
        # Check 1: Symbol validation if enabled
        if self._enable_symbol_validation:
            symbol_violations = self._validate_symbol(signal.symbol)
            violations.extend(symbol_violations)
        
        # Check 2: Exchange validation if enabled
        if self._enable_symbol_validation:
            exchange_violations = self._validate_exchange(signal.exchange)
            violations.extend(exchange_violations)
        
        # Check 3: Price sanity checks if enabled
        if self._enable_price_sanity:
            price_violations = self._validate_price(signal.price)
            violations.extend(price_violations)
        
        # Check 4: Confidence check if enabled
        if self._enable_confidence_check:
            confidence_violations = self._validate_confidence(signal)
            violations.extend(confidence_violations)
        
        # Check 5: Profitability check if enabled
        if self._enable_profitability:
            profit_violations = self._validate_profitability(signal)
            violations.extend(profit_violations)
        
        # Check 6: Required fields validation
        required_violations = self._validate_required_fields(signal)
        violations.extend(required_violations)
        
        logger.debug(
            "signal_validation_completed",
            signal_id=signal.signal_id,
            violation_count=len(violations),
            checks_performed={
                "symbol": self._enable_symbol_validation,
                "price_sanity": self._enable_price_sanity,
                "confidence": self._enable_confidence_check,
                "profitability": self._enable_profitability,
                "required_fields": True
            }
        )
        
        return violations
    
    def _validate_symbol(self, symbol: Symbol) -> List[str]:
        """Validate symbol object.
        
        Args:
            symbol: Symbol to validate
            
        Returns:
            List of violations
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Checks Symbol object type, NOT string
        - NO assumptions about symbol format
        """
        violations = []
        
        # Check if it's actually a Symbol object
        if not isinstance(symbol, Symbol):
            violations.append(
                f"Symbol must be Symbol object, got {type(symbol).__name__}"
            )
            return violations
        
        # Check symbol value is not empty
        if not symbol.value or not symbol.value.strip():
            violations.append("Symbol value cannot be empty")
        
        # Check symbol value length (reasonable bounds)
        symbol_value = symbol.value.strip()
        if len(symbol_value) < 2:
            violations.append(f"Symbol '{symbol_value}' too short (minimum 2 characters)")
        elif len(symbol_value) > 30:
            violations.append(f"Symbol '{symbol_value}' too long (maximum 30 characters)")
        
        return violations
    
    def _validate_exchange(self, exchange) -> List[str]:
        """Validate exchange parameter.
        
        Args:
            exchange: Exchange to validate (should be ExchangeName)
            
        Returns:
            List of violations
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Checks ExchangeName enum type, NOT string
        - NO assumptions about exchange availability
        """
        violations = []
        
        # Check if it's an ExchangeName enum
        if not isinstance(exchange, ExchangeName):
            violations.append(
                f"Exchange must be ExchangeName enum, got {type(exchange).__name__}"
            )
            return violations
        
        # Check if exchange is enabled in config
        exchange_config = self.config.exchanges.get(exchange.value)
        if not exchange_config:
            violations.append(f"Exchange '{exchange.value}' not configured")
        elif not exchange_config.enabled:
            violations.append(f"Exchange '{exchange.value}' is disabled")
        
        return violations
    
    def _validate_price(self, price) -> List[str]:
        """Validate price against configured ranges.
        
        Args:
            price: Price to validate
            
        Returns:
            List of violations
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses price ranges from config.risk.checkers.thresholds
        - NO hardcoded price bounds
        """
        violations = []
        
        if price is None:
            violations.append("Price cannot be None")
            return violations
        
        try:
            # Convert to Decimal for precise comparison
            from decimal import Decimal
            if not isinstance(price, Decimal):
                price = Decimal(str(price))
        except (ValueError, TypeError):
            violations.append(f"Price must be numeric, got {type(price).__name__}")
            return violations
        
        # Check price is positive
        if price <= 0:
            violations.append(f"Price must be positive, got {price}")
        
        # Check against configured bounds
        if price < self._min_price:
            violations.append(
                f"Price {price} below minimum {self._min_price}"
            )
        
        if price > self._max_price:
            violations.append(
                f"Price {price} above maximum {self._max_price}"
            )
        
        return violations
    
    def _validate_confidence(self, signal: TradeSignal) -> List[str]:
        """Validate signal confidence level.
        
        Args:
            signal: Signal to check confidence for
            
        Returns:
            List of violations
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses confidence threshold from config
        - NO hardcoded confidence requirements
        """
        violations = []
        
        # Check if signal has confidence attribute
        confidence = getattr(signal, 'confidence', None)
        
        if confidence is None:
            violations.append("Signal missing confidence level")
            return violations
        
        try:
            from decimal import Decimal
            if not isinstance(confidence, Decimal):
                confidence = Decimal(str(confidence))
        except (ValueError, TypeError):
            violations.append(f"Confidence must be numeric, got {type(confidence).__name__}")
            return violations
        
        # Check confidence is in valid range [0, 1]
        if confidence < 0 or confidence > 1:
            violations.append(f"Confidence {confidence} must be between 0 and 1")
        
        # Check against configured minimum
        if confidence < self._min_confidence:
            violations.append(
                f"Confidence {confidence} below minimum {self._min_confidence}"
            )
        
        return violations
    
    def _validate_profitability(self, signal: TradeSignal) -> List[str]:
        """Validate expected profitability.
        
        Args:
            signal: Signal to check profitability for
            
        Returns:
            List of violations
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses profitability threshold from config
        - NO hardcoded profit requirements
        """
        violations = []
        
        # Check if signal has expected profit
        expected_profit = getattr(signal, 'expected_profit', None)
        
        if expected_profit is None:
            violations.append("Signal missing expected profit")
            return violations
        
        try:
            from decimal import Decimal
            if not isinstance(expected_profit, Decimal):
                expected_profit = Decimal(str(expected_profit))
        except (ValueError, TypeError):
            violations.append(f"Expected profit must be numeric, got {type(expected_profit).__name__}")
            return violations
        
        # Check against configured minimum profitability
        if expected_profit < self._min_profitability:
            violations.append(
                f"Expected profit {expected_profit} below minimum {self._min_profitability}"
            )
        
        return violations
    
    def _validate_required_fields(self, signal: TradeSignal) -> List[str]:
        """Validate required signal fields.
        
        Args:
            signal: Signal to validate
            
        Returns:
            List of violations
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Checks all critical fields are present
        - NO assumptions about optional fields
        """
        violations = []
        
        # Check signal ID
        if not signal.signal_id:
            violations.append("Signal ID cannot be empty")
        
        # Check symbol (already checked above if symbol validation enabled)
        if not signal.symbol:
            violations.append("Symbol is required")
        
        # Check exchange (already checked above if symbol validation enabled)
        if not signal.exchange:
            violations.append("Exchange is required")
        
        # Check side
        if not signal.side:
            violations.append("Order side is required")
        
        # Check timestamp
        if not signal.timestamp:
            violations.append("Signal timestamp is required")
        
        return violations
    
    async def get_validation_stats(self) -> dict:
        """Get validation statistics.
        
        Returns:
            Dictionary with validation configuration and stats
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns explicit configuration state
        - NO hardcoded defaults in response
        """
        return {
            "validation_config": {
                "symbol_validation_enabled": self._enable_symbol_validation,
                "price_sanity_enabled": self._enable_price_sanity,
                "profitability_enabled": self._enable_profitability,
                "confidence_check_enabled": self._enable_confidence_check
            },
            "thresholds": {
                "min_confidence": float(self._min_confidence),
                "min_price": float(self._min_price),
                "max_price": float(self._max_price),
                "min_profitability": float(self._min_profitability)
            },
            "enabled_exchanges": [
                name for name, config in self.config.exchanges.items()
                if config.enabled
            ]
        }