"""cyberdelta.config.config_models.

------------------------------

Pydantic models for CyberDeltaEngine configuration validation.

These models define the structure, types, defaults, and validation rules for config.yaml,
leveraging utility functions from cyberdelta.utils.parsing for robust parsing and validation.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal, Self

from pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    ValidationInfo,
    computed_field,
    field_validator,
    model_validator,
)

# Import shared types and strategy models
from cyberdelta.config.models.config_types import (
    ConfigDecimal,
    NonEmptyConfigString,
)

# Import strategy models from separate module
from cyberdelta.config.models.funding_strategy_models import StrategiesSettings

# Import generator at top level now that circular dependency is resolved
from cyberdelta.config.models.smart_symbol_generator import SmartSymbolGenerator

# Import smart symbol models after other imports to avoid circular imports
from cyberdelta.config.models.smart_symbol_models import SmartSymbolsConfig

# Import symbol configs from separate module to avoid circular imports
from cyberdelta.config.models.symbol_configs import (
    SymbolGroupConfig,
)


if TYPE_CHECKING:
    from cyberdelta.config.models.smart_symbol_models import SmartSymbolsConfig
from cyberdelta.enums.environment import EnvironmentType
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.base import (
    ConfigurationError,
    RequiredParameterError,
    TestnetConfigurationError,
)
from cyberdelta.exceptions.field_validation import RangeFieldError
from cyberdelta.utils.parsing import (
    validate_enum_field,
    validate_str_field,
)


# Typed factory functions to avoid Unknown type inference
def _initial_positions_factory() -> list[dict[str, Any]]:
    """Factory function for initial_positions list.

    Returns:
        Empty list for initial positions configuration
    """
    return []


class AddressActionSafetyNetConfig(BaseModel):
    """Configuration for address-based action safety net rate limiting."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    rate_per_minute: int = Field(
        ...,
        gt=0,
        description="Client-side safety net rate for address-based actions, in actions per minute.",
    )


class GeneralSettings(BaseModel):
    """General application settings."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    log_level: Literal["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_file: NonEmptyConfigString | None = None
    module_log_levels: (
        dict[str, Literal["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"]] | None
    ) = None
    safe_mode: bool = True
    state_file: NonEmptyConfigString = "data/state.json"
    state_backup_directory: NonEmptyConfigString = "data/state_backups"
    state_save_interval: int = Field(default=300, gt=0)  # seconds
    state_backup_count: int = Field(default=5, gt=0)  # Number of previous state files to keep

    @field_validator("log_level", mode="before")
    @classmethod
    def _validate_log_level(cls, v: str | float | bool, info: ValidationInfo) -> str:
        return validate_enum_field(
            v,
            allowed={"INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"},
            field_name=info.field_name or "log_level",
        )

    @field_validator("module_log_levels", mode="before")
    @classmethod
    def _validate_module_log_levels(
        cls,
        v: dict[str, str] | list[str] | str | float | bool | None,
        info: ValidationInfo,
    ) -> dict[str, str] | None:
        """Validate module_log_levels dictionary structure and values.

        Args:
            v: The value to validate (dict, list, str, float, bool, or None)
            info: Validation context containing field information

        Returns:
            Validated dictionary of module names to log levels, or None

        Raises:
            TypeError: If value is not a dict or None
        """
        if v is None:
            return None

        if not isinstance(v, dict):
            field_name = info.field_name or "module_log_levels"
            msg = f"{field_name}: Expected dict or None, got {type(v).__name__}"
            raise TypeError(msg)

        validated_levels: dict[str, str] = {}
        allowed_levels = {"INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"}

        for raw_key, raw_value in v.items():
            # Validate module name
            validated_key = validate_str_field(
                raw_key,
                field_name=f"{info.field_name or 'module_log_levels'}.key",
                allow_empty=False,
            )
            # Validate log level value
            validated_value = validate_enum_field(
                raw_value,
                allowed=allowed_levels,
                field_name=f"{info.field_name or 'module_log_levels'}.{raw_key}",
            )
            validated_levels[validated_key] = validated_value

        return validated_levels


class ExchangeSpecificConfig(BaseModel):
    """Configuration for a specific exchange."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = True
    # Mainnet URLs (renamed from api_base_url and ws_url)
    api_base_url_mainnet: HttpUrl = Field(
        ...,
        description="Base URL for the exchange's mainnet REST API.",
    )
    ws_url_mainnet: AnyUrl = Field(
        ...,
        description="Base URL for the exchange's mainnet WebSocket API.",
    )
    # Testnet URLs (optional)
    api_base_url_testnet: HttpUrl | None = Field(
        default=None,
        description="Optional base URL for the exchange's testnet REST API.",
    )
    ws_url_testnet: AnyUrl | None = Field(
        default=None,
        description="Optional base URL for the exchange's testnet WebSocket API.",
    )
    # Environment type (replaces dangerous boolean is_mainnet_environment)
    environment_type: EnvironmentType = Field(
        default=EnvironmentType.MAINNET,
        description=(
            "Environment type for mainnet/testnet selection - replaces dangerous boolean flag"
        ),
    )
    rate_limit_per_minute: int | None = Field(
        default=None,
        gt=0,
        description="For simple exchanges: total requests per minute.",
    )
    symbols: dict[str, str] = Field(
        default_factory=dict,
        description="Legacy symbol mappings (optional, use unified_symbols instead)",
    )
    exchange_name: ExchangeName = Field(
        ...,
        description="Canonical exchange name, must match a value from ExchangeName enum.",
    )

    # Hyperliquid-specific rate limiting configuration
    ip_weight_limit_per_minute: int | None = Field(
        default=None,
        gt=0,
        description="Hyperliquid: Total IP weight budget per minute (e.g., 1200).",
    )
    info_request_type_ip_weights: dict[str, int] | None = Field(
        default=None,
        description="Hyperliquid: IP weights for /info request types. Keys are API 'type' strings.",
    )
    default_info_weight: int | None = Field(
        default=None,
        ge=1,
        description="Hyperliquid: Default IP weight for unlisted /info types.",
    )
    exchange_action_base_ip_weight: int | None = Field(
        default=None,
        ge=1,
        description="Hyperliquid: Base IP weight for one /exchange action.",
    )
    address_action_safety_net: AddressActionSafetyNetConfig | None = Field(
        default=None,
        description="Hyperliquid: Config for address action safety net limiter.",
    )
    websocket_send_rate_per_minute: int | None = Field(
        default=None,
        gt=0,
        description="Hyperliquid: Max outgoing WS messages (commands) per minute.",
    )

    # HTTP Client Settings (Optional overrides for HttpClientConfig defaults)
    request_timeout_seconds: float | None = Field(
        default=None,
        gt=0,
        le=120,
        description="Default request timeout in seconds for this exchange's HTTP client.",
    )
    max_retries: int | None = Field(
        default=None,
        ge=0,
        le=10,
        description="Maximum number of retries for failed HTTP requests for this exchange.",
    )
    retry_delay_seconds: float | None = Field(
        default=None,
        gt=0,
        le=300,
        description="Base delay in seconds for HTTP retries for this exchange.",
    )

    # WebSocket Manager Settings (Optional overrides for WebSocketManagerConfig defaults)
    ws_ping_interval_seconds: float | None = Field(
        default=None,
        gt=0,
        le=60,
        description="WebSocket ping interval in seconds for this exchange.",
    )
    ws_reconnect_delay_seconds: float | None = Field(
        default=None,
        gt=0,
        le=300,
        description="Base delay for WebSocket reconnections in seconds for this exchange.",
    )
    ws_max_reconnect_attempts: int | None = Field(
        default=None,
        ge=0,
        le=20,
        description="Maximum WebSocket reconnection attempts for this exchange.",
    )
    ws_connection_timeout_seconds: float | None = Field(
        default=None,
        gt=0,
        le=120,
        description="WebSocket connection timeout in seconds for this exchange.",
    )

    # Exchange-Specific Parameters
    chain_id: int | None = Field(
        default=None,
        gt=0,
        description="Blockchain Chain ID, required for some exchanges (e.g., Hyperliquid).",
    )

    @field_validator(
        "api_base_url_mainnet",
        "ws_url_mainnet",
        "api_base_url_testnet",
        "ws_url_testnet",
        mode="before",
    )
    @classmethod
    def _validate_url_strings(
        cls,
        v: str | float | bool | HttpUrl | AnyUrl | None,
        info: ValidationInfo,
    ) -> str | None:
        # Testnet URLs can be None
        if v is None and info.field_name and "testnet" in info.field_name:
            return None
        # Handle Pydantic URL objects
        if isinstance(v, HttpUrl | AnyUrl):
            return str(v)
        # Ensure it's a valid string before Pydantic URL validation
        return validate_str_field(v, field_name=info.field_name or "url_field", allow_empty=False)

    @field_validator("symbols", mode="before")
    @classmethod
    def _validate_symbols_dict(
        cls,
        v: dict[str, str] | list[str] | str | float | bool,
        info: ValidationInfo,
    ) -> dict[str, str]:
        """Validate symbols dictionary structure and values.

        Args:
            v: The value to validate (dict, list, str, float, or bool)
            info: Validation context containing field information

        Returns:
            Validated dictionary of symbol mappings

        Raises:
            TypeError: If value is not a dict
        """
        if not isinstance(v, dict):
            field_name = info.field_name or "symbols"
            msg = f"{field_name}: Expected dict, got {type(v).__name__}"
            raise TypeError(msg)

        validated_symbols: dict[str, str] = {}
        for raw_key, raw_value in v.items():
            validated_key = validate_str_field(
                raw_key,
                field_name=f"{info.field_name or 'symbols'}.key",
                allow_empty=False,
            )
            validated_value = validate_str_field(
                raw_value,
                field_name=f"{info.field_name or 'symbols'}.{raw_key}",
                allow_empty=False,
            )
            validated_symbols[validated_key] = validated_value

        return validated_symbols

    @model_validator(mode="after")
    def check_exchange_specific_rate_limit_configs(self) -> Self:
        """Validate that appropriate rate limit fields are present for each exchange type.

        Returns:
            Self instance after validation

        Raises:
            RequiredParameterError: If required rate limit fields are missing
        """
        if self.exchange_name == ExchangeName.HYPERLIQUID:
            # Hyperliquid requires its specific rate limit configuration
            required_fields = [
                "ip_weight_limit_per_minute",
                "info_request_type_ip_weights",
                "default_info_weight",
                "exchange_action_base_ip_weight",
                "address_action_safety_net",
            ]
            # websocket_send_rate_per_minute is optional
            for field_name in required_fields:
                field_value = getattr(self, field_name)
                if field_value is None:
                    raise RequiredParameterError(
                        parameter=field_name,
                        context="ExchangeSpecificConfig for Hyperliquid",
                        exchange="hyperliquid",
                    )
        elif self.exchange_name == ExchangeName.BACKPACK:
            # Backpack requires the simple rate_limit_per_minute
            if self.rate_limit_per_minute is None:
                raise RequiredParameterError(
                    parameter="rate_limit_per_minute",
                    context="ExchangeSpecificConfig for Backpack",
                    exchange="backpack",
                )

        return self

    @model_validator(mode="after")
    def check_testnet_urls_when_needed(self) -> Self:
        """Validate that testnet URLs are provided when environment is testnet.

        Returns:
            Self instance after validation

        Raises:
            TestnetConfigurationError: If testnet URLs are missing when required
        """
        if self.environment_type == EnvironmentType.TESTNET:
            if self.api_base_url_testnet is None:
                raise TestnetConfigurationError(
                    missing_config="api_base_url_testnet",
                    config_type="API URL",
                )
            if self.ws_url_testnet is None:
                raise TestnetConfigurationError(
                    missing_config="ws_url_testnet",
                    config_type="WebSocket URL",
                )
        return self

    @property
    def active_api_base_url(self) -> HttpUrl:
        """Return the active API base URL based on environment setting.

        Returns:
            Active API base URL for the configured environment

        Raises:
            TestnetConfigurationError: If testnet URL is required but not configured
        """
        if self.environment_type == EnvironmentType.MAINNET:
            return self.api_base_url_mainnet
        if self.api_base_url_testnet is None:
            raise TestnetConfigurationError(
                missing_config="api_base_url_testnet",
                config_type="API URL",
            )
        return self.api_base_url_testnet

    @property
    def active_ws_url(self) -> AnyUrl:
        """Return the active WebSocket URL based on environment setting.

        Returns:
            Active WebSocket URL for the configured environment

        Raises:
            TestnetConfigurationError: If testnet URL is required but not configured
        """
        if self.environment_type == EnvironmentType.MAINNET:
            return self.ws_url_mainnet
        if self.ws_url_testnet is None:
            raise TestnetConfigurationError(
                missing_config="ws_url_testnet",
                config_type="WebSocket URL",
            )
        return self.ws_url_testnet


class GlobalRiskSettings(BaseModel):
    """Global risk management settings."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_position_usd: ConfigDecimal = Field(..., gt=Decimal(0))
    max_total_exposure_usd: ConfigDecimal = Field(..., gt=Decimal(0))


class CheckerThresholds(BaseModel):
    """Complete strongly typed checker thresholds covering all risk module needs."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Profitability thresholds
    min_profitability: ConfigDecimal = Field(default=Decimal("0.001"), gt=Decimal(0))

    # Price sanity thresholds
    max_price_deviation: ConfigDecimal = Field(default=Decimal("0.1"), gt=Decimal(0), le=Decimal(1))
    max_price_spread: ConfigDecimal = Field(default=Decimal("0.05"), gt=Decimal(0), le=Decimal(1))
    min_price: ConfigDecimal = Field(default=Decimal("0.0000001"), gt=Decimal(0))
    max_price: ConfigDecimal = Field(default=Decimal(1000000), gt=Decimal(0))
    outlier_z_score_threshold: float = Field(default=3.0, gt=0, le=10)

    # Funding rate thresholds
    min_funding_rate: ConfigDecimal = Field(
        default=Decimal("-0.01"), ge=Decimal(-1), le=Decimal(0)
    )  # -1%
    max_funding_rate: ConfigDecimal = Field(default=Decimal("0.01"), gt=Decimal(0), le=Decimal(1))
    max_funding_rate_spread: ConfigDecimal = Field(default=Decimal("0.005"), gt=Decimal(0))
    max_funding_rate_volatility: ConfigDecimal = Field(
        default=Decimal("0.002"), gt=Decimal(0), le=Decimal(1)
    )  # 0.2%
    min_funding_rate_confidence: ConfigDecimal = Field(
        default=Decimal("0.7"), ge=Decimal(0), le=Decimal(1)
    )  # 70%

    # Volatility thresholds
    max_volatility: ConfigDecimal = Field(default=Decimal("0.2"), gt=Decimal(0), le=Decimal(2))
    min_volatility: ConfigDecimal = Field(default=Decimal("0.001"), gt=Decimal(0))

    # Balance thresholds
    min_balance_ratio: ConfigDecimal = Field(default=Decimal("0.1"), gt=Decimal(0), le=Decimal(1))

    @model_validator(mode="after")
    def validate_threshold_relationships(self) -> Self:
        """Validate logical relationships between thresholds.

        Returns:
            Self instance after validation

        Raises:
            ValueError: If threshold relationships are invalid
        """
        if self.min_profitability >= self.max_price_spread:
            msg = "min_profitability must be less than max_price_spread"
            raise ValueError(msg)
        if self.min_price >= self.max_price:
            msg = "min_price must be less than max_price"
            raise ValueError(msg)
        return self


class CheckerSettings(BaseModel):
    """Enhanced checker configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Enable/disable flags
    enable_required_fields: bool = True
    enable_profitability: bool = True
    enable_circuit_breaker: bool = True
    enable_price_sanity: bool = True
    enable_funding_rate: bool = True
    enable_volatility: bool = True
    enable_balance: bool = True

    # Thresholds
    thresholds: CheckerThresholds = Field(default_factory=CheckerThresholds)

    # Pipeline configuration
    fail_fast: bool = True
    max_concurrent_checks: int = Field(default=5, gt=0, le=20)
    check_timeout_seconds: float = Field(default=5.0, gt=0, le=60)

    # Lookback periods
    funding_rate_lookback_hours: int = Field(default=24, gt=0, le=168)
    volatility_lookback_hours: int = Field(default=24, gt=0, le=168)

    # Feature flags
    include_fees_in_profitability: bool = True
    enable_outlier_detection: bool = True
    check_both_exchanges: bool = True

    # Funding rate specific settings
    enable_funding_rate_stability_check: bool = True
    require_primary_funding_source: bool = True

    # Extensibility (preserve from current CheckConfig)
    extra_config: dict[str, Any] | None = None


class SizingSettings(BaseModel):
    """Enhanced sizing configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Method selection
    method: Literal["kelly", "simple"] = "simple"

    # Kelly criterion parameters
    kelly_multiplier: ConfigDecimal = Field(default=Decimal("0.25"), gt=Decimal(0), le=Decimal(1))
    kelly_max_allocation: ConfigDecimal = Field(
        default=Decimal("0.1"), gt=Decimal(0), le=Decimal(1)
    )
    kelly_min_allocation: ConfigDecimal = Field(
        default=Decimal("0.01"), gt=Decimal(0), le=Decimal(1)
    )
    kelly_risk_free_rate: float = Field(default=0.02, ge=0, le=1)  # Annual rate

    # Simple sizing parameters
    simple_method: Literal["fixed_usd", "fixed_fraction"] = "fixed_fraction"
    simple_fixed_usd: ConfigDecimal = Field(default=Decimal(1000), gt=Decimal(0))
    simple_fixed_fraction: ConfigDecimal = Field(
        default=Decimal("0.02"), gt=Decimal(0), le=Decimal(1)
    )

    # Position limits
    min_position_size: ConfigDecimal = Field(default=Decimal(100), gt=Decimal(0))
    max_position_size: ConfigDecimal = Field(default=Decimal(10000), gt=Decimal(0))
    max_leverage: ConfigDecimal = Field(default=Decimal("5.0"), gt=Decimal(1))

    # Portfolio limits
    max_portfolio_allocation: ConfigDecimal = Field(
        default=Decimal("0.5"), gt=Decimal(0), le=Decimal(1)
    )
    total_capital: ConfigDecimal | None = None

    # Volatility bounds and adjustment factors
    min_volatility: ConfigDecimal = Field(default=Decimal("0.001"), gt=Decimal(0))
    max_volatility_bound: ConfigDecimal = Field(default=Decimal("1.0"), gt=Decimal(0))
    volatility_lookback_hours: int = Field(default=24, gt=0, le=168)

    # Validation factors
    enable_validation_factors: bool = True
    enable_volatility_adjustment: bool = True
    enable_spread_adjustment: bool = True
    base_validation_factor: ConfigDecimal = Field(
        default=Decimal("0.8"), gt=Decimal(0), le=Decimal(1)
    )

    # Timing configuration
    sizing_timeout_seconds: float = Field(default=10.0, gt=0)

    @field_validator("method", mode="before")
    @classmethod
    def _validate_method(cls, v: str | float | bool, info: ValidationInfo) -> str:
        return validate_enum_field(
            v,
            allowed={"kelly", "simple"},
            field_name=info.field_name or "method",
        )

    @field_validator("simple_method", mode="before")
    @classmethod
    def _validate_simple_method(cls, v: str | float | bool, info: ValidationInfo) -> str:
        return validate_enum_field(
            v,
            allowed={"fixed_usd", "fixed_fraction"},
            field_name=info.field_name or "simple_method",
        )

    @model_validator(mode="after")
    def validate_allocation_ranges(self) -> Self:
        """Validate allocation ranges are logical.

        Returns:
            Self instance after validation

        Raises:
            ValueError: If allocation ranges are invalid
        """
        if self.kelly_min_allocation >= self.kelly_max_allocation:
            msg = "kelly_min_allocation must be less than kelly_max_allocation"
            raise ValueError(msg)
        if self.min_position_size >= self.max_position_size:
            msg = "min_position_size must be less than max_position_size"
            raise ValueError(msg)
        return self


class EnhancedRiskSettings(BaseModel):
    """Complete risk management configuration preserving existing GlobalRiskSettings."""

    model_config = ConfigDict(extra="forbid", frozen=True, validate_assignment=True)

    # CRITICAL: Preserve existing GlobalRiskSettings integration
    global_risk: GlobalRiskSettings = Field(..., alias="global")

    # Enhanced checker and sizing configuration
    checkers: CheckerSettings = Field(default_factory=CheckerSettings)
    sizing: SizingSettings = Field(default_factory=SizingSettings)

    # BACKWARD COMPATIBILITY: Preserve legacy simple sizing fields during migration
    use_simple_sizing_path: bool = True
    simple_sizing_method: Literal["fixed_usd", "fixed_fraction"] = "fixed_fraction"
    simple_fixed_fraction: ConfigDecimal = Field(
        default=Decimal("0.1"), gt=Decimal(0), lt=Decimal(1)
    )
    simple_fixed_usd_size: ConfigDecimal = Field(default=Decimal("10.0"), gt=Decimal(0))

    # System configuration
    enabled: bool = True
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_all_checks: bool = False
    log_performance_metrics: bool = True

    # Concurrency limits
    max_concurrent_checks: int = Field(default=10, gt=0, le=50)
    max_concurrent_sizing: int = Field(default=5, gt=0, le=20)

    @field_validator("simple_sizing_method", mode="before")
    @classmethod
    def _validate_sizing_method(cls, v: str | float | bool, info: ValidationInfo) -> str:
        return validate_enum_field(
            v,
            allowed={"fixed_usd", "fixed_fraction"},
            field_name=info.field_name or "simple_sizing_method",
        )

    @field_validator("log_level", mode="before")
    @classmethod
    def _validate_log_level(cls, v: str | float | bool, info: ValidationInfo) -> str:
        return validate_enum_field(
            v,
            allowed={"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"},
            field_name=info.field_name or "log_level",
        )

    @model_validator(mode="after")
    def validate_cross_settings(self) -> Self:
        """Validate relationships between different settings and GlobalRiskSettings.

        Returns:
            Self instance after validation

        Raises:
            ValueError: If cross-setting relationships are invalid
        """
        # Ensure system-level concurrency is higher than component level
        if self.max_concurrent_checks < self.checkers.max_concurrent_checks:
            msg = "System max_concurrent_checks must be >= checkers.max_concurrent_checks"
            raise ValueError(msg)

        # Validate sizing limits don't exceed global risk limits
        if (
            hasattr(self.global_risk, "max_position_usd")
            and self.sizing.max_position_size > self.global_risk.max_position_usd
        ):
            msg = "Sizing max_position_size cannot exceed global_risk.max_position_usd"
            raise ValueError(msg)

        return self


# Alias for backward compatibility during migration
RiskSettings = EnhancedRiskSettings


class ExecutionCompensationSettings(BaseModel):
    """Execution compensation settings."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    use_limit_orders: bool = True
    limit_price_offset_pct: ConfigDecimal = Field(default=Decimal("0.05"), ge=Decimal(0))


class ExecutionSettings(BaseModel):
    """Execution configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_slippage_pct: ConfigDecimal = Field(..., gt=Decimal(0), lt=Decimal(1))
    max_retries: int = Field(default=3, gt=0)
    retry_delay_base_sec: ConfigDecimal = Field(default=Decimal("1.0"), gt=Decimal(0))
    settlement_delay: ConfigDecimal = Field(default=Decimal("2.0"), ge=Decimal(0))
    compensation: ExecutionCompensationSettings


class CircuitBreakerSettings(BaseModel):
    """Circuit breaker configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = True
    global_consecutive_failures: int = Field(default=5, gt=0)
    global_reset_timeout_sec: int = Field(default=300, gt=0)
    exchange_consecutive_failures: int = Field(default=3, gt=0)
    exchange_reset_timeout_sec: int = Field(default=180, gt=0)


class PositionReconciliationSettings(BaseModel):
    """Position reconciliation configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = True
    check_interval_sec: int = Field(default=600, gt=0)
    max_discrepancy_pct: ConfigDecimal = Field(
        default=Decimal("0.01"), ge=Decimal(0), lt=Decimal(1)
    )


class BalanceMonitoringSettings(BaseModel):
    """Balance monitoring configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = True
    check_interval_sec: int = Field(default=300, gt=0)
    min_balance_thresholds_usd: dict[str, ConfigDecimal]

    @field_validator("min_balance_thresholds_usd", mode="before")
    @classmethod
    def _validate_balance_thresholds_keys(
        cls,
        v: dict[str, str | int | float | Decimal] | list[str] | str | float | bool,
        info: ValidationInfo,
    ) -> dict[str, str | int | float | Decimal]:
        """Validate dictionary structure and keys before ConfigDecimal processes values.

        Args:
            v: The value to validate (dict, list, str, float, bool, or Decimal)
            info: Validation context containing field information

        Returns:
            Validated dictionary with string keys and numeric values

        Raises:
            TypeError: If value is not a dict
        """
        if not isinstance(v, dict):
            field_name = info.field_name or "min_balance_thresholds_usd"
            msg = f"{field_name}: Expected dict, got {type(v).__name__}"
            raise TypeError(msg)

        validated_thresholds: dict[str, str | int | float | Decimal] = {}
        for raw_key, raw_value in v.items():
            validated_key = validate_str_field(
                raw_key,
                field_name=f"{info.field_name or 'min_balance_thresholds_usd'}.key",
                allow_empty=False,
            )
            validated_thresholds[validated_key] = raw_value

        return validated_thresholds

    @field_validator("min_balance_thresholds_usd", mode="after")
    @classmethod
    def _validate_balance_thresholds_values(
        cls,
        v: dict[str, Decimal],
        info: ValidationInfo,
    ) -> dict[str, Decimal]:
        """Validate that all Decimal values are positive after ConfigDecimal parsing.

        Args:
            v: Dictionary with validated Decimal values
            info: Validation context containing field information

        Returns:
            Validated dictionary with positive Decimal values

        Raises:
            RangeFieldError: If any Decimal value is not positive
        """
        for key, value in v.items():
            if value <= Decimal(0):
                raise RangeFieldError(
                    field_name=f"{info.field_name or 'min_balance_thresholds_usd'}.{key}",
                    value=value,
                    min_value=0.0,
                    constraint="Balance threshold must be positive",
                )
        return v


class SafetySystemsSettings(BaseModel):
    """Safety systems configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    circuit_breakers: CircuitBreakerSettings
    position_reconciliation: PositionReconciliationSettings
    balance_monitoring: BalanceMonitoringSettings


def _default_alert_methods() -> list[Literal["log", "telegram"]]:
    """Create default factory for alert_methods field.

    Returns the default list of alert methods for monitoring configuration.
    Used as a factory function to avoid mutable default arguments.

    Returns:
        List containing default alert methods (currently just "log").

    """
    return ["log"]


class MonitoringSettings(BaseModel):
    """Monitoring and notifications configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    notifications_enabled: bool = True
    alert_methods: list[Literal["log", "telegram"]] = Field(default_factory=_default_alert_methods)

    @field_validator("alert_methods", mode="before")
    @classmethod
    def _validate_alert_methods(
        cls,
        v: list[str | int | float | bool] | str | float | bool,
        info: ValidationInfo,
    ) -> list[str]:
        if not isinstance(v, list):
            field_name = info.field_name or "alert_methods"
            msg = f"{field_name}: Expected list, got {type(v).__name__}"
            raise TypeError(msg)

        validated_methods: list[str] = []
        for i, raw_method in enumerate(v):
            validated_method = validate_enum_field(
                raw_method,
                allowed={"log", "telegram"},
                field_name=f"{info.field_name or 'alert_methods'}[{i}]",
            )
            validated_methods.append(validated_method)

        return validated_methods


# Default timeout for how long balance/position data is considered fresh
DEFAULT_DATA_FRESHNESS_SECONDS = 60

# Type aliases for portfolio tracker config
type ExchangeId = str


class PortfolioCacheSettings(BaseModel):
    """Portfolio cache configuration settings."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = True
    max_size: int = Field(default=10000, gt=0, le=100000)
    default_ttl: float = Field(default=300.0, gt=0, le=3600)
    stale_while_revalidate: float = Field(default=60.0, gt=0, le=600)
    cleanup_interval: float = Field(default=600.0, gt=0, le=3600)
    enable_memory_optimization: bool = True
    cache_statistics_enabled: bool = True


class PortfolioStateSettings(BaseModel):
    """Portfolio state management settings."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # State persistence
    persist_interval: float = Field(default=60.0, gt=0, le=600)
    backup_count: int = Field(default=5, gt=0, le=20)
    backup_directory: NonEmptyConfigString = "data/portfolio_backups"

    # State update settings
    atomic_updates: bool = True
    update_timeout: float = Field(default=5.0, gt=0, le=30)
    max_concurrent_updates: int = Field(default=1, gt=0, le=10)

    # State validation
    validate_on_load: bool = True
    validate_on_update: bool = True
    strict_validation: bool = False

    # State history
    max_state_history_size: int = Field(default=100, gt=0, le=1000)
    max_trade_history_size: int = Field(default=1000, gt=0, le=10000)


class PortfolioValidationSettings(BaseModel):
    """Portfolio validation configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Balance validation
    enable_balance_validation: bool = True
    balance_tolerance: ConfigDecimal = Field(default=Decimal("0.0001"), gt=Decimal(0))
    require_non_negative_balances: bool = True

    # Position validation
    enable_position_validation: bool = True
    position_size_tolerance: ConfigDecimal = Field(default=Decimal("0.0001"), gt=Decimal(0))
    max_position_age_seconds: int = Field(default=300, gt=0)

    # Trade validation
    enable_trade_validation: bool = True
    max_trade_age_seconds: int = Field(default=86400, gt=0)  # 24 hours
    require_valid_timestamps: bool = True

    # Cross-validation
    enable_cross_validation: bool = True
    validation_timeout: float = Field(default=10.0, gt=0, le=60)

    # Price and quantity validation
    min_price: ConfigDecimal = Field(default=Decimal("0.0000001"), gt=Decimal(0))
    max_price: ConfigDecimal = Field(default=Decimal("1000000.0"), gt=Decimal(0))
    min_quantity: ConfigDecimal = Field(default=Decimal("0.00000001"), gt=Decimal(0))
    max_quantity: ConfigDecimal = Field(default=Decimal("1000000.0"), gt=Decimal(0))
    min_trade_value: ConfigDecimal = Field(default=Decimal("0.01"), gt=Decimal(0))
    max_trade_value: ConfigDecimal = Field(default=Decimal("10000000.0"), gt=Decimal(0))

    # Balance thresholds
    min_balance_threshold: ConfigDecimal = Field(default=Decimal("0.00001"), ge=Decimal(0))
    allow_negative_balances: bool = False

    # Position limits
    max_position_size: ConfigDecimal = Field(default=Decimal("100000.0"), gt=Decimal(0))
    max_leverage: ConfigDecimal = Field(default=Decimal("10.0"), gt=Decimal(0))
    max_position_value: ConfigDecimal = Field(default=Decimal("1000000.0"), gt=Decimal(0))

    # Other settings
    max_recent_issues: int = Field(default=100, gt=0)
    enabled: bool = True
    strict_mode: bool = False


class PortfolioCalculationSettings(BaseModel):
    """Portfolio calculation configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # PnL calculation
    pnl_calculation_method: Literal["fifo", "lifo", "weighted_average"] = "weighted_average"
    include_fees_in_pnl: bool = True
    include_funding_in_pnl: bool = True

    # Exposure calculation
    max_exposure_calculation_depth: int = Field(default=100, gt=0, le=1000)
    group_by_base_asset: bool = True
    exposure_update_interval: float = Field(default=5.0, gt=0, le=60)

    # Performance metrics
    calculate_sharpe_ratio: bool = True
    sharpe_lookback_days: int = Field(default=30, gt=0, le=365)
    calculate_max_drawdown: bool = True
    performance_update_interval: float = Field(default=300.0, gt=0, le=3600)
    risk_free_rate: ConfigDecimal = Field(default=Decimal("0.02"), ge=Decimal(0), le=Decimal("0.1"))

    # Exposure calculation settings
    default_volatility: ConfigDecimal = Field(
        default=Decimal("0.2"), gt=Decimal(0), le=Decimal("5.0")
    )
    stress_scenario_move: ConfigDecimal = Field(
        default=Decimal("0.1"), gt=Decimal(0), le=Decimal("1.0")
    )
    var_confidence_level: ConfigDecimal = Field(
        default=Decimal("0.95"), gt=Decimal("0.5"), lt=Decimal("1.0")
    )
    leverage_warning_threshold: ConfigDecimal = Field(
        default=Decimal("3.0"), gt=Decimal(0), le=Decimal("50.0")
    )

    # PnL calculation
    realized_pnl_method: Literal["fifo", "lifo", "weighted_average"] = "weighted_average"

    # Price service settings
    price_cache_ttl: int = Field(default=60, gt=0, le=3600)
    batch_size_limit: int = Field(default=100, gt=0, le=1000)
    price_staleness_threshold: int = Field(default=300, gt=0, le=3600)


# PortfolioTrackerConfig removed - replaced by modular portfolio system


class AppSettings(BaseModel):
    """Root configuration model for CyberDeltaEngine."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    general: GeneralSettings
    exchanges: dict[str, ExchangeSpecificConfig]
    strategies: StrategiesSettings
    risk: EnhancedRiskSettings  # Using EnhancedRiskSettings directly
    execution: ExecutionSettings
    safety_systems: SafetySystemsSettings
    monitoring: MonitoringSettings
    # portfolio_tracker field removed - replaced by modular portfolio system
    # CLEAN BREAK: Smart symbol configuration replaces verbose unified_symbols
    symbols: "SmartSymbolsConfig" = Field(..., description="Smart symbol configuration")

    @property
    @computed_field
    def symbol_groups(self) -> list[SymbolGroupConfig]:
        """Generate symbol groups from smart configuration.

        Uses the new unified symbol architecture with SymbolGroupConfig.
        """
        generator = SmartSymbolGenerator(self.symbols)
        return generator.generate_symbol_groups()

    @field_validator("exchanges", mode="before")
    @classmethod
    def _validate_exchanges_dict(
        cls,
        v: dict[str, dict[str, str | int | float | bool]] | list[str] | str | float | bool,
        info: ValidationInfo,
    ) -> dict[str, dict[str, str | int | float | bool]]:
        """Validate exchanges dictionary structure and values.

        Args:
            v: The value to validate (dict, list, str, float, or bool)
            info: Validation context containing field information

        Returns:
            Validated dictionary of exchange configurations

        Raises:
            TypeError: If value is not a dict
        """
        if not isinstance(v, dict):
            field_name = info.field_name or "exchanges"
            msg = f"{field_name}: Expected dict, got {type(v).__name__}"
            raise TypeError(msg)

        # Validate exchange names are non-empty strings
        validated_exchanges: dict[str, dict[str, str | int | float | bool]] = {}
        for raw_key, raw_value in v.items():
            # Type checker knows raw_key is str after isinstance check above
            validated_key = validate_str_field(
                raw_key,
                field_name=f"{info.field_name or 'exchanges'}.key",
                allow_empty=False,
            )
            validated_exchanges[validated_key] = raw_value

        return validated_exchanges

    @model_validator(mode="after")
    def _validate_cross_references(self) -> Self:
        """Validate cross-references between configuration sections.

        Returns:
            Self instance after validation

        Raises:
            ConfigurationError: If cross-references are invalid
        """
        # Ensure strategy exchanges exist in exchanges config
        strategy = self.strategies.hl_perp_bp_spot
        if strategy.long_exchange not in self.exchanges:
            raise ConfigurationError(
                message=(
                    f"Strategy long_exchange '{strategy.long_exchange}' "
                    f"not found in exchanges configuration"
                ),
                metadata={"exchange": strategy.long_exchange, "context": "strategy_configuration"},
            )
        if strategy.short_exchange not in self.exchanges:
            raise ConfigurationError(
                message=(
                    f"Strategy short_exchange '{strategy.short_exchange}' "
                    f"not found in exchanges configuration"
                ),
                metadata={"exchange": strategy.short_exchange, "context": "strategy_configuration"},
            )

        # Ensure balance monitoring thresholds reference valid exchanges
        balance_exchanges = set(
            self.safety_systems.balance_monitoring.min_balance_thresholds_usd.keys(),
        )
        configured_exchanges = set(self.exchanges.keys())
        invalid_exchanges = balance_exchanges - configured_exchanges
        if invalid_exchanges:
            raise ConfigurationError(
                message=(
                    f"Balance monitoring references unknown exchanges: {sorted(invalid_exchanges)}"
                ),
                metadata={
                    "invalid_exchanges": list(invalid_exchanges),
                    "context": "balance_monitoring",
                },
            )

        return self

    @model_validator(mode="after")
    def validate_exchange_config_keys_match_names(self) -> Self:
        """Validate that exchange configuration keys match their exchange_name field values.

        Returns:
            Self instance after validation

        Raises:
            ConfigurationError: If configuration keys don't match exchange names
        """
        if self.exchanges:  # Check if exchanges dict is not None and not empty
            for key, exchange_cfg_instance in self.exchanges.items():
                # Compare the string key with the string value of the ExchangeName enum member
                if exchange_cfg_instance.exchange_name.value != key:
                    raise ConfigurationError(
                        message=(
                            f"Exchange configuration key-name mismatch for '{key}': "
                            f"dictionary key is '{key}', but 'exchange_name' field "
                            f"is '{exchange_cfg_instance.exchange_name.value}'. "
                            f"These must be identical (e.g., 'hyperliquid' key must have "
                            f"'hyperliquid' as exchange_name)."
                        ),
                        metadata={
                            "dict_key": key,
                            "exchange_name_field": exchange_cfg_instance.exchange_name.value,
                            "context": "exchange_config_validation",
                        },
                    )
        return self

    @model_validator(mode="after")
    def validate_hyperliquid_chain_id_settings(self) -> Self:
        """Validate that chain_id is specified for enabled Hyperliquid exchange.

        Returns:
            Self instance after validation

        Raises:
            RequiredParameterError: If chain_id is missing for enabled Hyperliquid
            RangeFieldError: If chain_id value is invalid
        """
        if self.exchanges:
            hyperliquid_config = self.exchanges.get("hyperliquid")
            if hyperliquid_config and hyperliquid_config.enabled:
                if hyperliquid_config.chain_id is None:
                    raise RequiredParameterError(
                        parameter="chain_id",
                        context="AppSettings for enabled 'hyperliquid' exchange",
                        exchange="hyperliquid",
                    )
                # DEFENSIVE CHECK: Verify chain_id is positive after Field validation.
                # Since chain_id is validated as int | None with gt=0, we just need
                # to check it's positive (redundant but kept as defensive programming)
                if hyperliquid_config.chain_id <= 0:
                    raise RangeFieldError(
                        field_name="chain_id",
                        value=hyperliquid_config.chain_id,
                        min_value=1,
                        constraint=(
                            "'chain_id' for 'hyperliquid' exchange must be a positive integer"
                        ),
                    )
        return self


# Model rebuild to resolve forward references
AppSettings.model_rebuild()
