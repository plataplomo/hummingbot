"""cyberdelta.config.config_models.

------------------------------

Pydantic models for CyberDeltaEngine configuration validation.

These models define the structure, types, defaults, and validation rules for config.yaml,
leveraging utility functions from cyberdelta.utils.parsing for robust parsing and validation.
"""

from decimal import Decimal
from typing import Any, Literal, Self

from pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    ValidationInfo,
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
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.parsing import (
    validate_enum_field,
    validate_str_field,
)


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
    state_save_interval: int = Field(300, gt=0)  # seconds
    state_backup_count: int = Field(5, gt=0)  # Number of previous state files to keep

    @field_validator("log_level", mode="before")
    @classmethod
    def _validate_log_level(cls, v: str | int | float | bool, info: ValidationInfo) -> str:
        return validate_enum_field(
            v,
            allowed={"INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"},
            field_name=info.field_name or "log_level",
        )

    @field_validator("module_log_levels", mode="before")
    @classmethod
    def _validate_module_log_levels(
        cls,
        v: dict[str, str] | list[str] | str | int | float | bool | None,
        info: ValidationInfo,
    ) -> dict[str, str] | None:
        """Validate module_log_levels dictionary structure and values."""
        if v is None:
            return None

        if not isinstance(v, dict):
            raise ValueError(
                f"{info.field_name or 'module_log_levels'}: Expected dict or None, "
                f"got {type(v).__name__}",
            )

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
    # Environment flag
    is_mainnet_environment: bool = Field(
        default=True,
        description=(
            "If True, mainnet URLs are used. If False, testnet URLs are used (if provided)."
        ),
    )
    rate_limit_per_minute: int | None = Field(
        default=None,
        gt=0,
        description="For simple exchanges: total requests per minute.",
    )
    symbols: dict[str, str]
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
        v: str | int | float | bool | None | HttpUrl | AnyUrl,
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
        v: dict[str, str] | list[str] | str | int | float | bool,
        info: ValidationInfo,
    ) -> dict[str, str]:
        if not isinstance(v, dict):
            raise ValueError(
                f"{info.field_name or 'symbols'}: Expected dict, got {type(v).__name__}",
            )

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
        """Validate that appropriate rate limit fields are present for each exchange type."""
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
                    raise ValueError(
                        f"ExchangeSpecificConfig for Hyperliquid: '{field_name}' is required "
                        f"but not provided.",
                    )
        elif self.exchange_name == ExchangeName.BACKPACK:
            # Backpack requires the simple rate_limit_per_minute
            if self.rate_limit_per_minute is None:
                raise ValueError(
                    "ExchangeSpecificConfig for Backpack: 'rate_limit_per_minute' is required "
                    "but not provided.",
                )

        return self

    @model_validator(mode="after")
    def check_testnet_urls_when_needed(self) -> Self:
        """Validate that testnet URLs are provided when is_mainnet_environment is False."""
        if not self.is_mainnet_environment:
            if self.api_base_url_testnet is None:
                raise ValueError(
                    f"ExchangeSpecificConfig for {self.exchange_name.value}: "
                    f"'api_base_url_testnet' is required when 'is_mainnet_environment' is False.",
                )
            if self.ws_url_testnet is None:
                raise ValueError(
                    f"ExchangeSpecificConfig for {self.exchange_name.value}: "
                    f"'ws_url_testnet' is required when 'is_mainnet_environment' is False.",
                )
        return self

    @property
    def active_api_base_url(self) -> HttpUrl:
        """Return the active API base URL based on environment setting."""
        if self.is_mainnet_environment:
            return self.api_base_url_mainnet
        if self.api_base_url_testnet is None:
            raise ValueError(
                f"Cannot get active_api_base_url for {self.exchange_name.value}: "
                f"testnet environment requested but api_base_url_testnet is None",
            )
        return self.api_base_url_testnet

    @property
    def active_ws_url(self) -> AnyUrl:
        """Return the active WebSocket URL based on environment setting."""
        if self.is_mainnet_environment:
            return self.ws_url_mainnet
        if self.ws_url_testnet is None:
            raise ValueError(
                f"Cannot get active_ws_url for {self.exchange_name.value}: "
                f"testnet environment requested but ws_url_testnet is None",
            )
        return self.ws_url_testnet


class GlobalRiskSettings(BaseModel):
    """Global risk management settings."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_position_usd: ConfigDecimal = Field(..., gt=Decimal("0"))
    max_total_exposure_usd: ConfigDecimal = Field(..., gt=Decimal("0"))


class RiskSettings(BaseModel):
    """Risk management configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    global_risk: GlobalRiskSettings = Field(..., alias="global")
    use_simple_sizing_path: bool = True
    simple_sizing_method: Literal["fixed_usd", "fixed_fraction"] = "fixed_fraction"
    simple_fixed_fraction: ConfigDecimal = Field(Decimal("0.1"), gt=Decimal("0"), lt=Decimal("1"))
    simple_fixed_usd_size: ConfigDecimal = Field(Decimal("10.0"), gt=Decimal("0"))

    @field_validator("simple_sizing_method", mode="before")
    @classmethod
    def _validate_sizing_method(cls, v: str | int | float | bool, info: ValidationInfo) -> str:
        return validate_enum_field(
            v,
            allowed={"fixed_usd", "fixed_fraction"},
            field_name=info.field_name or "simple_sizing_method",
        )


class ExecutionCompensationSettings(BaseModel):
    """Execution compensation settings."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    use_limit_orders: bool = True
    limit_price_offset_pct: ConfigDecimal = Field(Decimal("0.05"), ge=Decimal("0"))


class ExecutionSettings(BaseModel):
    """Execution configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_slippage_pct: ConfigDecimal = Field(..., gt=Decimal("0"), lt=Decimal("1"))
    max_retries: int = Field(3, gt=0)
    retry_delay_base_sec: ConfigDecimal = Field(Decimal("1.0"), gt=Decimal("0"))
    settlement_delay: ConfigDecimal = Field(Decimal("2.0"), ge=Decimal("0"))
    compensation: ExecutionCompensationSettings


class CircuitBreakerSettings(BaseModel):
    """Circuit breaker configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = True
    global_consecutive_failures: int = Field(5, gt=0)
    global_reset_timeout_sec: int = Field(300, gt=0)
    exchange_consecutive_failures: int = Field(3, gt=0)
    exchange_reset_timeout_sec: int = Field(180, gt=0)


class PositionReconciliationSettings(BaseModel):
    """Position reconciliation configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = True
    check_interval_sec: int = Field(600, gt=0)
    max_discrepancy_pct: ConfigDecimal = Field(Decimal("0.01"), ge=Decimal("0"), lt=Decimal("1"))


class BalanceMonitoringSettings(BaseModel):
    """Balance monitoring configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = True
    check_interval_sec: int = Field(300, gt=0)
    min_balance_thresholds_usd: dict[str, ConfigDecimal]

    @field_validator("min_balance_thresholds_usd", mode="before")
    @classmethod
    def _validate_balance_thresholds_keys(
        cls,
        v: dict[str, str | int | float | Decimal] | list[str] | str | int | float | bool,
        info: ValidationInfo,
    ) -> dict[str, str | int | float | Decimal]:
        """Validate dictionary structure and keys before ConfigDecimal processes values."""
        if not isinstance(v, dict):
            raise ValueError(
                f"{info.field_name or 'min_balance_thresholds_usd'}: "
                f"Expected dict, got {type(v).__name__}",
            )

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
        """Validate that all Decimal values are positive after ConfigDecimal parsing."""
        for key, value in v.items():
            if value <= Decimal("0"):
                raise ValueError(
                    f"Field '{info.field_name or 'min_balance_thresholds_usd'}.{key}': "
                    f"Balance threshold must be positive, got {value}.",
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
        v: list[str | int | float | bool] | str | int | float | bool,
        info: ValidationInfo,
    ) -> list[str]:
        if not isinstance(v, list):
            raise ValueError(
                f"{info.field_name or 'alert_methods'}: Expected list, got {type(v).__name__}",
            )

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


class PortfolioTrackerConfig(BaseModel):
    """Portfolio tracker configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    data_freshness_seconds: int = Field(DEFAULT_DATA_FRESHNESS_SECONDS, gt=0)
    initial_balances: dict[ExchangeId, dict[str, str]] = Field(default_factory=dict)
    initial_positions: list[dict[str, Any]] = Field(default_factory=lambda: [])


class AppSettings(BaseModel):
    """Root configuration model for CyberDeltaEngine."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    general: GeneralSettings
    exchanges: dict[str, ExchangeSpecificConfig]
    strategies: StrategiesSettings
    risk: RiskSettings
    execution: ExecutionSettings
    safety_systems: SafetySystemsSettings
    monitoring: MonitoringSettings
    portfolio_tracker: PortfolioTrackerConfig

    @field_validator("exchanges", mode="before")
    @classmethod
    def _validate_exchanges_dict(
        cls,
        v: dict[str, dict[str, str | int | float | bool]] | list[str] | str | int | float | bool,
        info: ValidationInfo,
    ) -> dict[str, dict[str, str | int | float | bool]]:
        if not isinstance(v, dict):
            raise ValueError(
                f"{info.field_name or 'exchanges'}: Expected dict, got {type(v).__name__}",
            )

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
        """Validate cross-references between configuration sections."""
        # Ensure strategy exchanges exist in exchanges config
        strategy = self.strategies.hl_perp_bp_spot
        if strategy.long_exchange not in self.exchanges:
            raise ValueError(
                f"Strategy long_exchange '{strategy.long_exchange}' "
                f"not found in exchanges configuration",
            )
        if strategy.short_exchange not in self.exchanges:
            raise ValueError(
                f"Strategy short_exchange '{strategy.short_exchange}' "
                f"not found in exchanges configuration",
            )

        # Ensure balance monitoring thresholds reference valid exchanges
        balance_exchanges = set(
            self.safety_systems.balance_monitoring.min_balance_thresholds_usd.keys(),
        )
        configured_exchanges = set(self.exchanges.keys())
        invalid_exchanges = balance_exchanges - configured_exchanges
        if invalid_exchanges:
            raise ValueError(
                f"Balance monitoring references unknown exchanges: {sorted(invalid_exchanges)}",
            )

        return self

    @model_validator(mode="after")
    def validate_exchange_config_keys_match_names(self) -> Self:
        """Validate that exchange configuration keys match their exchange_name field values."""
        if self.exchanges:  # Check if exchanges dict is not None and not empty
            for key, exchange_cfg_instance in self.exchanges.items():
                # Compare the string key with the string value of the ExchangeName enum member
                if exchange_cfg_instance.exchange_name.value != key:
                    raise ValueError(
                        f"Exchange configuration key-name mismatch for '{key}': "
                        f"dictionary key is '{key}', but 'exchange_name' field "
                        f"is '{exchange_cfg_instance.exchange_name.value}'. "
                        f"These must be identical (e.g., 'hyperliquid' key must have "
                        f"'hyperliquid' as exchange_name).",
                    )
        return self

    @model_validator(mode="after")
    def validate_hyperliquid_chain_id_settings(self) -> Self:
        """Validate that chain_id is specified for enabled Hyperliquid exchange."""
        if self.exchanges:
            hyperliquid_config = self.exchanges.get("hyperliquid")
            if hyperliquid_config and hyperliquid_config.enabled:
                if hyperliquid_config.chain_id is None:
                    raise ValueError(
                        "AppSettings: 'chain_id' must be specified in config.yaml for "
                        "enabled 'hyperliquid' exchange.",
                    )
                # DEFENSIVE CHECK: Verify chain_id is positive after Field validation.
                # Since chain_id is validated as int | None with gt=0, we just need
                # to check it's positive (redundant but kept as defensive programming)
                if hyperliquid_config.chain_id <= 0:
                    raise ValueError(
                        "AppSettings: 'chain_id' for 'hyperliquid' exchange must be a "
                        "positive integer.",
                    )
        return self
