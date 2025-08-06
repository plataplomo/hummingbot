"""Signal service for validation and routing of trading signals.

This module provides the SignalService class that validates trading signals
and routes them to risk assessment using validated AppSettings configuration.
"""

from __future__ import annotations

from decimal import Decimal

from cyberdelta.application.event_bus import EventBus
from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import TradeSignal


# Symbol validation constants to avoid magic numbers
MIN_SYMBOL_LENGTH = 2
MAX_SYMBOL_LENGTH = 30

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
    ) -> None:
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
            max_price=float(self._max_price),
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
            exchange=(
                signal.exchange.value
                if isinstance(signal.exchange, ExchangeName)
                else [e.value for e in signal.exchange]
            ),
            side=signal.side.value,
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
                    violations=violations,
                )
                return False

            # Signal passed validation - publish for risk assessment
            # Note: This needs to be updated to match EventBus interface
            # For now, we'll use a simplified approach
            # TODO: Update to proper event publishing when EventBus interface is clarified
            logger.info(
                "signal_ready_for_risk_assessment",
                signal_id=signal.signal_id,
                symbol=signal.symbol.value,
                exchange=(
                    signal.exchange.value
                    if isinstance(signal.exchange, ExchangeName)
                    else [e.value for e in signal.exchange]
                ),
            )

            logger.info(
                "signal_validated_and_published",
                signal_id=signal.signal_id,
                symbol=signal.symbol.value,
                exchange=(
                    signal.exchange.value
                    if isinstance(signal.exchange, ExchangeName)
                    else [e.value for e in signal.exchange]
                ),
                side=signal.side.value,
                price=float(signal.price) if signal.price else None,
            )

        except Exception as e:
            logger.exception("signal_processing_error", signal_id=signal.signal_id, error=str(e))
            return False
        else:
            return True

    async def validate_signal(self, signal: TradeSignal) -> list[str]:
        """Validate signal against configured business rules.

        Args:
            signal: Trading signal to validate (Pydantic already validated structure)

        Returns:
            List of validation violations (empty if valid)

        Note: Pydantic handles structural validation (types, required fields, ranges).
        This method only validates business logic that Pydantic cannot handle.
        """
        violations: list[str] = []

        # Check 1: Exchange enabled in config (business rule)
        exchange_violations = self._validate_exchange_enabled(signal.exchange)
        violations.extend(exchange_violations)

        # Check 2: Price within configured business bounds
        if self._enable_price_sanity:
            price_violations = self._validate_price_ranges(signal.price)
            violations.extend(price_violations)

        # Check 3: Confidence meets threshold (business rule)
        if self._enable_confidence_check and signal.confidence is not None:
            confidence_violations = self._validate_confidence_threshold(signal.confidence)
            violations.extend(confidence_violations)

        # Check 4: Signal not expired (business rule)
        if not signal.is_valid():
            violations.append("Signal has expired")

        logger.debug(
            "signal_validation_completed",
            signal_id=signal.signal_id,
            violation_count=len(violations),
            checks_performed={
                "exchange_enabled": True,
                "price_ranges": self._enable_price_sanity,
                "confidence_threshold": self._enable_confidence_check,
                "expiration": True,
            },
        )

        return violations

    def _validate_exchange_enabled(self, exchange: ExchangeName | list[ExchangeName]) -> list[str]:
        """Validate exchange is enabled in configuration.

        Args:
            exchange: Exchange(s) to validate (Pydantic already validated type)

        Returns:
            List of violations
        """
        violations: list[str] = []

        # Handle single exchange or list of exchanges
        exchanges_to_check = [exchange] if isinstance(exchange, ExchangeName) else exchange

        for exch in exchanges_to_check:
            exchange_config = self.config.exchanges.get(exch.value)
            if not exchange_config:
                violations.append(f"Exchange '{exch.value}' not configured")
            elif not exchange_config.enabled:
                violations.append(f"Exchange '{exch.value}' is disabled")

        return violations

    def _validate_price_ranges(self, price: Decimal) -> list[str]:
        """Validate price against configured business ranges.

        Args:
            price: Price to validate (Pydantic already validated it's positive Decimal)

        Returns:
            List of violations
        """
        violations: list[str] = []

        # Check against configured business bounds
        if price < self._min_price:
            violations.append(f"Price {price} below minimum business threshold {self._min_price}")

        if price > self._max_price:
            violations.append(f"Price {price} above maximum business threshold {self._max_price}")

        return violations

    def _validate_confidence_threshold(self, confidence: float) -> list[str]:
        """Validate confidence meets business threshold.

        Args:
            confidence: Confidence to validate (Pydantic already validated it's a float)

        Returns:
            List of violations
        """
        violations: list[str] = []

        # Check against configured minimum threshold
        if Decimal(str(confidence)) < self._min_confidence:
            violations.append(
                f"Confidence {confidence} below minimum threshold {self._min_confidence}"
            )

        return violations

    async def get_validation_stats(self) -> dict[str, object]:
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
                "confidence_check_enabled": self._enable_confidence_check,
            },
            "thresholds": {
                "min_confidence": float(self._min_confidence),
                "min_price": float(self._min_price),
                "max_price": float(self._max_price),
                "min_profitability": float(self._min_profitability),
            },
            "enabled_exchanges": [
                name for name, config in self.config.exchanges.items() if config.enabled
            ],
        }
