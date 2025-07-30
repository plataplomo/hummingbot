"""Configuration validation for ExecutionHandler services.

This module provides comprehensive validation of application settings
to ensure safe and correct operation of the execution system.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config.structlog_config import TraceLevelLogger, get_logger
from cyberdelta.core.services.interfaces import BaseService


# Configuration validation constants
_MAX_REASONABLE_RETRIES = 10
_MAX_REASONABLE_RETRY_DELAY_SEC = 60
_MIN_EXCHANGES_FOR_ARBITRAGE = 2
_HIGH_SLIPPAGE_WARNING_PCT = Decimal("0.05")  # 5%
_HIGH_COMPENSATION_OFFSET_WARNING_PCT = Decimal("0.1")  # 10%
_LOW_POSITION_SIZE_WARNING_USD = 10


if TYPE_CHECKING:
    from cyberdelta.config.models.config_models import (
        AppSettings,
        ExchangeSpecificConfig,
        ExecutionCompensationSettings,
        ExecutionSettings,
        RiskSettings,
    )


@dataclass
class ConfigValidationResult:
    """Result of configuration validation."""

    is_valid: bool
    errors: list[str]
    warnings: list[str]
    critical_errors: list[str]  # Errors that prevent system startup


class ExecutionConfigValidator(BaseService):
    """Validates ExecutionHandler configuration for safety and correctness."""

    def __init__(self, logger: TraceLevelLogger | None = None) -> None:
        """Initialize configuration validator.

        Args:
            logger: Optional logger instance
        """
        super().__init__(logger)
        self.logger = logger or get_logger(__name__)

    def validate_app_settings(self, settings: AppSettings) -> ConfigValidationResult:
        """Comprehensive validation of application settings.

        Args:
            settings: Application settings to validate

        Returns:
            ConfigValidationResult with validation outcome
        """
        errors: list[str] = []
        warnings: list[str] = []
        critical_errors: list[str] = []

        # 1. Validate execution settings
        exec_errors, exec_warnings, exec_critical = self._validate_execution_settings(settings)
        errors.extend(exec_errors)
        warnings.extend(exec_warnings)
        critical_errors.extend(exec_critical)

        # 2. Validate exchange settings
        exchange_errors, exchange_warnings, exchange_critical = self._validate_exchange_settings(
            settings
        )
        errors.extend(exchange_errors)
        warnings.extend(exchange_warnings)
        critical_errors.extend(exchange_critical)

        # 3. Validate risk settings
        risk_errors, risk_warnings, risk_critical = self._validate_risk_settings(settings)
        errors.extend(risk_errors)
        warnings.extend(risk_warnings)
        critical_errors.extend(risk_critical)

        # 4. Validate cross-dependencies
        cross_errors, cross_warnings, cross_critical = self._validate_cross_dependencies(settings)
        errors.extend(cross_errors)
        warnings.extend(cross_warnings)
        critical_errors.extend(cross_critical)

        # Log validation results
        self._log_validation_results(errors, warnings, critical_errors)

        return ConfigValidationResult(
            is_valid=len(critical_errors) == 0,
            errors=errors,
            warnings=warnings,
            critical_errors=critical_errors,
        )

    def _validate_execution_settings(
        self, settings: AppSettings
    ) -> tuple[list[str], list[str], list[str]]:
        """Validate execution-specific settings.

        Returns:
            Tuple of (errors, warnings, critical_errors)
        """
        errors: list[str] = []
        warnings: list[str] = []
        critical_errors: list[str] = []

        if not hasattr(settings, "execution") or not settings.execution:
            critical_errors.append(
                "Missing execution settings - cannot initialize ExecutionHandler"
            )
            return errors, warnings, critical_errors

        exec_settings: ExecutionSettings = settings.execution

        # Validate retry configuration
        retry_errors, retry_warnings, retry_critical = self._validate_retry_settings(exec_settings)
        errors.extend(retry_errors)
        warnings.extend(retry_warnings)
        critical_errors.extend(retry_critical)

        # Validate slippage settings
        slippage_errors, slippage_warnings, slippage_critical = self._validate_slippage_settings(
            exec_settings
        )
        errors.extend(slippage_errors)
        warnings.extend(slippage_warnings)
        critical_errors.extend(slippage_critical)

        # Validate other execution settings
        other_errors, other_warnings, other_critical = self._validate_other_execution_settings(
            exec_settings
        )
        errors.extend(other_errors)
        warnings.extend(other_warnings)
        critical_errors.extend(other_critical)

        return errors, warnings, critical_errors

    def _validate_retry_settings(
        self, exec_settings: ExecutionSettings
    ) -> tuple[list[str], list[str], list[str]]:
        """Validate retry-related settings.

        Returns:
            Tuple of (errors, warnings, critical_errors) lists
        """
        errors: list[str] = []
        warnings: list[str] = []
        critical_errors: list[str] = []

        # Validate retry count
        if hasattr(exec_settings, "max_retries"):
            if exec_settings.max_retries < 0:
                critical_errors.append(
                    f"max_retries cannot be negative: {exec_settings.max_retries}"
                )
            elif exec_settings.max_retries > _MAX_REASONABLE_RETRIES:
                warnings.append(f"max_retries is very high: {exec_settings.max_retries}")
        else:
            errors.append("Missing max_retries in execution settings")

        # Validate retry delay
        if hasattr(exec_settings, "retry_delay_base_sec"):
            if exec_settings.retry_delay_base_sec <= 0:
                critical_errors.append(
                    f"retry_delay_base_sec must be positive: {exec_settings.retry_delay_base_sec}"
                )
            elif exec_settings.retry_delay_base_sec > _MAX_REASONABLE_RETRY_DELAY_SEC:
                warnings.append(
                    f"retry_delay_base_sec is very high: {exec_settings.retry_delay_base_sec}s"
                )
        else:
            errors.append("Missing retry_delay_base_sec in execution settings")

        return errors, warnings, critical_errors

    def _validate_slippage_settings(
        self, exec_settings: ExecutionSettings
    ) -> tuple[list[str], list[str], list[str]]:
        """Validate slippage-related settings.

        Returns:
            Tuple of (errors, warnings, critical_errors) lists
        """
        errors: list[str] = []
        warnings: list[str] = []
        critical_errors: list[str] = []

        if hasattr(exec_settings, "max_slippage_pct"):
            slippage = exec_settings.max_slippage_pct
            if slippage <= 0:
                critical_errors.append(f"max_slippage_pct must be positive: {slippage}")
            elif slippage >= 1:
                critical_errors.append(f"max_slippage_pct cannot be >= 100%: {slippage}")
            elif slippage > _HIGH_SLIPPAGE_WARNING_PCT:
                warnings.append(f"max_slippage_pct is very high: {slippage * 100}%")
        else:
            errors.append("Missing max_slippage_pct in execution settings")

        return errors, warnings, critical_errors

    def _validate_other_execution_settings(
        self, exec_settings: ExecutionSettings
    ) -> tuple[list[str], list[str], list[str]]:
        """Validate other execution settings.

        Returns:
            Tuple of (errors, warnings, critical_errors) lists
        """
        errors: list[str] = []
        warnings: list[str] = []
        critical_errors: list[str] = []

        # Validate settlement delay
        if hasattr(exec_settings, "settlement_delay"):
            if exec_settings.settlement_delay < 0:
                critical_errors.append(
                    f"settlement_delay cannot be negative: {exec_settings.settlement_delay}"
                )
        else:
            errors.append("Missing settlement_delay in execution settings")

        # Validate compensation settings
        if hasattr(exec_settings, "compensation"):
            comp_errors, comp_warnings, comp_critical = self._validate_compensation_settings(
                exec_settings.compensation
            )
            errors.extend(comp_errors)
            warnings.extend(comp_warnings)
            critical_errors.extend(comp_critical)
        else:
            warnings.append("Missing compensation settings - using defaults")

        return errors, warnings, critical_errors

    def _validate_compensation_settings(
        self, comp_settings: ExecutionCompensationSettings
    ) -> tuple[list[str], list[str], list[str]]:
        """Validate compensation-specific settings.

        Returns:
            Tuple of (errors, warnings, critical_errors) lists
        """
        errors: list[str] = []
        warnings: list[str] = []
        critical_errors: list[str] = []

        # Validate limit price offset
        if hasattr(comp_settings, "limit_price_offset_pct"):
            offset = comp_settings.limit_price_offset_pct
            if offset < 0:
                critical_errors.append(f"limit_price_offset_pct cannot be negative: {offset}")
            elif offset > _HIGH_COMPENSATION_OFFSET_WARNING_PCT:
                warnings.append(f"limit_price_offset_pct is very high: {offset * 100}%")

        return errors, warnings, critical_errors

    def _validate_exchange_settings(
        self, settings: AppSettings
    ) -> tuple[list[str], list[str], list[str]]:
        """Validate exchange configuration settings.

        Returns:
            Tuple of (errors, warnings, critical_errors) lists
        """
        errors: list[str] = []
        warnings: list[str] = []
        critical_errors: list[str] = []

        if not hasattr(settings, "exchanges") or not settings.exchanges:
            critical_errors.append("No exchange configurations found")
            return errors, warnings, critical_errors

        enabled_exchanges: list[str] = []

        for exchange_id, exchange_config in settings.exchanges.items():
            if not exchange_config.enabled:
                continue

            enabled_exchanges.append(exchange_id)

            # Validate exchange-specific settings
            exchange_errors, exchange_warnings, exchange_critical = self._validate_single_exchange(
                exchange_id, exchange_config
            )
            errors.extend(exchange_errors)
            warnings.extend(exchange_warnings)
            critical_errors.extend(exchange_critical)

        # Check minimum exchanges for arbitrage
        if len(enabled_exchanges) < _MIN_EXCHANGES_FOR_ARBITRAGE:
            critical_errors.append(
                f"Need at least {_MIN_EXCHANGES_FOR_ARBITRAGE} enabled exchanges "
                f"for arbitrage, found: {len(enabled_exchanges)}"
            )

        return errors, warnings, critical_errors

    def _validate_single_exchange(
        self, exchange_id: str, exchange_config: ExchangeSpecificConfig
    ) -> tuple[list[str], list[str], list[str]]:
        """Validate settings for a single exchange.

        Returns:
            Tuple of (errors, warnings, critical_errors) lists
        """
        errors: list[str] = []
        warnings: list[str] = []
        critical_errors: list[str] = []

        # Validate required URLs
        if not exchange_config.api_base_url_mainnet:
            critical_errors.append(f"Missing api_base_url_mainnet for {exchange_id}")

        if not exchange_config.ws_url_mainnet:
            critical_errors.append(f"Missing ws_url_mainnet for {exchange_id}")

        # Validate testnet configuration if needed
        if not exchange_config.environment_type.is_production:
            if not exchange_config.api_base_url_testnet:
                critical_errors.append(
                    f"Missing api_base_url_testnet for {exchange_id} (testnet mode)"
                )
            if not exchange_config.ws_url_testnet:
                critical_errors.append(f"Missing ws_url_testnet for {exchange_id} (testnet mode)")

        # Validate exchange-specific rate limiting
        rate_errors, rate_warnings, rate_critical = self._validate_exchange_rate_limits(
            exchange_id, exchange_config
        )
        errors.extend(rate_errors)
        warnings.extend(rate_warnings)
        critical_errors.extend(rate_critical)

        # Validate symbols configuration
        if not exchange_config.symbols:
            warnings.append(f"No symbols configured for {exchange_id}")

        return errors, warnings, critical_errors

    def _validate_exchange_rate_limits(
        self, exchange_id: str, exchange_config: ExchangeSpecificConfig
    ) -> tuple[list[str], list[str], list[str]]:
        """Validate exchange-specific rate limiting configuration.

        Returns:
            Tuple of (errors, warnings, critical_errors) lists
        """
        errors: list[str] = []
        warnings: list[str] = []
        critical_errors: list[str] = []

        if exchange_config.exchange_name.value == "hyperliquid":
            # Hyperliquid-specific validation
            if not exchange_config.ip_weight_limit_per_minute:
                critical_errors.append("Missing ip_weight_limit_per_minute for Hyperliquid")
            elif exchange_config.ip_weight_limit_per_minute <= 0:
                critical_errors.append(
                    "ip_weight_limit_per_minute must be positive for Hyperliquid"
                )

            if not exchange_config.info_request_type_ip_weights:
                errors.append("Missing info_request_type_ip_weights for Hyperliquid")

            if not exchange_config.chain_id:
                critical_errors.append("Missing chain_id for Hyperliquid")
            elif exchange_config.chain_id <= 0:
                critical_errors.append(
                    f"chain_id must be positive for Hyperliquid: {exchange_config.chain_id}"
                )

        elif exchange_config.exchange_name.value == "backpack":
            # Backpack-specific validation
            if not exchange_config.rate_limit_per_minute:
                critical_errors.append("Missing rate_limit_per_minute for Backpack")
            elif exchange_config.rate_limit_per_minute <= 0:
                critical_errors.append("rate_limit_per_minute must be positive for Backpack")

        return errors, warnings, critical_errors

    def _validate_risk_settings(
        self, settings: AppSettings
    ) -> tuple[list[str], list[str], list[str]]:
        """Validate risk management settings.

        Returns:
            Tuple of (errors, warnings, critical_errors) lists
        """
        errors: list[str] = []
        warnings: list[str] = []
        critical_errors: list[str] = []

        if not hasattr(settings, "risk") or not settings.risk:
            critical_errors.append("Missing risk settings")
            return errors, warnings, critical_errors

        risk_settings: RiskSettings = settings.risk

        # Validate global risk settings
        global_errors, global_warnings, global_critical = self._validate_global_risk_settings(
            risk_settings
        )
        errors.extend(global_errors)
        warnings.extend(global_warnings)
        critical_errors.extend(global_critical)

        # Validate sizing settings
        sizing_errors, sizing_warnings, sizing_critical = self._validate_sizing_settings(
            risk_settings
        )
        errors.extend(sizing_errors)
        warnings.extend(sizing_warnings)
        critical_errors.extend(sizing_critical)

        return errors, warnings, critical_errors

    def _validate_global_risk_settings(
        self, risk_settings: RiskSettings
    ) -> tuple[list[str], list[str], list[str]]:
        """Validate global risk settings.

        Returns:
            Tuple of (errors, warnings, critical_errors) lists
        """
        errors: list[str] = []
        warnings: list[str] = []
        critical_errors: list[str] = []

        if hasattr(risk_settings, "global_risk"):
            global_risk = risk_settings.global_risk

            if hasattr(global_risk, "max_position_usd"):
                if global_risk.max_position_usd <= 0:
                    critical_errors.append(
                        f"max_position_usd must be positive: {global_risk.max_position_usd}"
                    )
                elif global_risk.max_position_usd < _LOW_POSITION_SIZE_WARNING_USD:
                    warnings.append(f"max_position_usd is very low: {global_risk.max_position_usd}")

            if hasattr(global_risk, "max_total_exposure_usd"):
                if global_risk.max_total_exposure_usd <= 0:
                    critical_errors.append(
                        f"max_total_exposure_usd must be positive: "
                        f"{global_risk.max_total_exposure_usd}"
                    )

                # Check that total exposure > max position
                if (
                    hasattr(global_risk, "max_position_usd")
                    and global_risk.max_total_exposure_usd < global_risk.max_position_usd
                ):
                    critical_errors.append(
                        f"max_total_exposure_usd ({global_risk.max_total_exposure_usd}) "
                        f"cannot be less than max_position_usd "
                        f"({global_risk.max_position_usd})"
                    )
        else:
            critical_errors.append("Missing global_risk settings")

        return errors, warnings, critical_errors

    def _validate_sizing_settings(
        self, risk_settings: RiskSettings
    ) -> tuple[list[str], list[str], list[str]]:
        """Validate position sizing settings.

        Returns:
            Tuple of (errors, warnings, critical_errors) lists
        """
        errors: list[str] = []
        warnings: list[str] = []
        critical_errors: list[str] = []

        if (
            hasattr(risk_settings, "use_simple_sizing_path")
            and risk_settings.use_simple_sizing_path
            and hasattr(risk_settings, "simple_sizing_method")
        ):
            method = risk_settings.simple_sizing_method

            if method == "fixed_fraction":
                if hasattr(risk_settings, "simple_fixed_fraction"):
                    fraction = risk_settings.simple_fixed_fraction
                    if fraction <= 0 or fraction >= 1:
                        critical_errors.append(
                            f"simple_fixed_fraction must be between 0 and 1: {fraction}"
                        )
                else:
                    critical_errors.append(
                        "Missing simple_fixed_fraction for fixed_fraction method"
                    )

            elif method == "fixed_usd":
                if hasattr(risk_settings, "simple_fixed_usd_size"):
                    size = risk_settings.simple_fixed_usd_size
                    if size <= 0:
                        critical_errors.append(f"simple_fixed_usd_size must be positive: {size}")
                else:
                    critical_errors.append("Missing simple_fixed_usd_size for fixed_usd method")

        return errors, warnings, critical_errors

    def _validate_cross_dependencies(
        self, settings: AppSettings
    ) -> tuple[list[str], list[str], list[str]]:
        """Validate cross-dependencies between configuration sections.

        Returns:
            Tuple of (errors, warnings, critical_errors) lists
        """
        errors: list[str] = []
        warnings: list[str] = []
        critical_errors: list[str] = []

        # Validate strategy-exchange dependencies
        strategy_errors, strategy_warnings, strategy_critical = (
            self._validate_strategy_dependencies(settings)
        )
        errors.extend(strategy_errors)
        warnings.extend(strategy_warnings)
        critical_errors.extend(strategy_critical)

        # Validate balance monitoring dependencies
        monitoring_errors, monitoring_warnings, monitoring_critical = (
            self._validate_monitoring_dependencies(settings)
        )
        errors.extend(monitoring_errors)
        warnings.extend(monitoring_warnings)
        critical_errors.extend(monitoring_critical)

        return errors, warnings, critical_errors

    def _validate_strategy_dependencies(
        self, settings: AppSettings
    ) -> tuple[list[str], list[str], list[str]]:
        """Validate strategy-exchange dependencies.

        Returns:
            Tuple of (errors, warnings, critical_errors) lists
        """
        errors: list[str] = []
        warnings: list[str] = []
        critical_errors: list[str] = []

        if (
            hasattr(settings, "strategies")
            and settings.strategies
            and hasattr(settings.strategies, "hl_perp_bp_spot")
        ):
            strategy = settings.strategies.hl_perp_bp_spot

            if strategy.enabled:
                # Check exchange references
                if strategy.long_exchange not in settings.exchanges:
                    critical_errors.append(
                        f"Strategy references unknown long_exchange: {strategy.long_exchange}"
                    )
                elif not settings.exchanges[strategy.long_exchange].enabled:
                    critical_errors.append(
                        f"Strategy references disabled long_exchange: {strategy.long_exchange}"
                    )

                if strategy.short_exchange not in settings.exchanges:
                    critical_errors.append(
                        f"Strategy references unknown short_exchange: {strategy.short_exchange}"
                    )
                elif not settings.exchanges[strategy.short_exchange].enabled:
                    critical_errors.append(
                        f"Strategy references disabled short_exchange: {strategy.short_exchange}"
                    )

                # Check symbol configurations
                if (
                    strategy.long_exchange in settings.exchanges
                    and strategy.symbol_long
                    not in settings.exchanges[strategy.long_exchange].symbols
                ):
                    errors.append(
                        f"Symbol {strategy.symbol_long} not configured for {strategy.long_exchange}"
                    )

                if (
                    strategy.short_exchange in settings.exchanges
                    and strategy.symbol_short
                    not in settings.exchanges[strategy.short_exchange].symbols
                ):
                    errors.append(
                        f"Symbol {strategy.symbol_short} not configured for "
                        f"{strategy.short_exchange}"
                    )

        return errors, warnings, critical_errors

    def _validate_monitoring_dependencies(
        self, settings: AppSettings
    ) -> tuple[list[str], list[str], list[str]]:
        """Validate balance monitoring dependencies.

        Returns:
            Tuple of (errors, warnings, critical_errors) lists
        """
        errors: list[str] = []
        warnings: list[str] = []
        critical_errors: list[str] = []

        if (
            hasattr(settings, "safety_systems")
            and settings.safety_systems
            and hasattr(settings.safety_systems, "balance_monitoring")
        ):
            balance_monitoring = settings.safety_systems.balance_monitoring

            if balance_monitoring.enabled:
                # Check that all monitored exchanges exist
                for exchange_id in balance_monitoring.min_balance_thresholds_usd:
                    if exchange_id not in settings.exchanges:
                        errors.append(
                            f"Balance monitoring references unknown exchange: {exchange_id}"
                        )
                    elif not settings.exchanges[exchange_id].enabled:
                        warnings.append(
                            f"Balance monitoring configured for disabled exchange: {exchange_id}"
                        )

        return errors, warnings, critical_errors

    def _log_validation_results(
        self, errors: list[str], warnings: list[str], critical_errors: list[str]
    ) -> None:
        """Log validation results with appropriate levels."""
        if critical_errors:
            self.logger.critical(
                "Configuration validation failed - critical errors found",
                critical_error_count=len(critical_errors),
                critical_errors=critical_errors,
                total_errors=len(errors) + len(critical_errors),
                total_warnings=len(warnings),
            )
        elif errors:
            self.logger.error(
                "Configuration validation found errors",
                error_count=len(errors),
                errors=errors,
                warning_count=len(warnings),
                warnings=warnings,
            )
        elif warnings:
            self.logger.warning(
                "Configuration validation found warnings",
                warning_count=len(warnings),
                warnings=warnings,
            )
        else:
            self.logger.info("Configuration validation passed successfully")


class ConfigValidationError(Exception):
    """Raised when configuration validation fails."""

    def __init__(self, validation_result: ConfigValidationResult) -> None:
        """Initialize with validation result.

        Args:
            validation_result: Failed validation result
        """
        self.validation_result = validation_result

        error_summary = (
            f"Configuration validation failed with "
            f"{len(validation_result.critical_errors)} critical errors"
        )
        super().__init__(error_summary)


def validate_execution_config(
    settings: AppSettings, logger: TraceLevelLogger | None = None
) -> None:
    """Validate execution configuration and raise exception if invalid.

    Args:
        settings: Application settings to validate
        logger: Optional logger instance

    Raises:
        ConfigValidationError: If validation fails with critical errors
    """
    validator = ExecutionConfigValidator(logger)
    result = validator.validate_app_settings(settings)

    if not result.is_valid:
        raise ConfigValidationError(result)
