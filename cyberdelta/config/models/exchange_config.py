"""Exchange-specific configuration models.

This module contains Pydantic models for exchange-specific settings,
including API endpoints, rate limits, and connection parameters.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Self

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

from cyberdelta.config.models.fee_config import FeeStructureConfig
from cyberdelta.enums.environment import EnvironmentType
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.base import (
    RequiredParameterError,
    TestnetConfigurationError,
)
from cyberdelta.utils.parsing import (
    validate_str_field,
)


if TYPE_CHECKING:
    pass


class AddressActionSafetyNetConfig(BaseModel):
    """Configuration for address-based action safety net rate limiting."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    rate_per_minute: int = Field(
        ...,
        gt=0,
        description="Client-side safety net rate for address-based actions, in actions per minute.",
    )


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

    # Trading constraints
    min_order_size: float | None = Field(
        default=None,
        gt=0,
        description="Minimum order size for this exchange in base currency.",
    )
    max_order_size: float | None = Field(
        default=None,
        gt=0,
        description="Maximum order size for this exchange in base currency.",
    )
    tick_size: float | None = Field(
        default=None,
        gt=0,
        description="Price tick size for this exchange.",
    )
    lot_size: float | None = Field(
        default=None,
        gt=0,
        description="Quantity lot size for this exchange.",
    )
    min_quantity: float | None = Field(
        default=None,
        gt=0,
        description="Minimum quantity for orders on this exchange.",
    )
    max_quantity: float | None = Field(
        default=None,
        gt=0,
        description="Maximum quantity for orders on this exchange.",
    )
    max_price_deviation_pct: float | None = Field(
        default=None,
        gt=0,
        le=1.0,
        description="Maximum price deviation from market price as percentage.",
    )
    min_order_book_depth: int | None = Field(
        default=None,
        gt=0,
        description="Minimum order book depth required for order placement.",
    )
    allow_order_modifications: bool | None = Field(
        default=None,
        description="Whether this exchange allows order modifications.",
    )
    max_price_change_pct: float | None = Field(
        default=None,
        gt=0,
        le=1.0,
        description="Maximum price change allowed for order modifications as percentage.",
    )
    max_quantity_change_pct: float | None = Field(
        default=None,
        gt=0,
        le=1.0,
        description="Maximum quantity change allowed for order modifications as percentage.",
    )
    allow_order_cancellations: bool | None = Field(
        default=None,
        description="Whether this exchange allows order cancellations.",
    )
    cancellable_statuses: list[str] | None = Field(
        default=None,
        description="List of order statuses that can be cancelled on this exchange.",
    )

    # Fee structure configuration
    fee_structure: FeeStructureConfig | None = Field(
        default=None,
        description="Fee structure configuration for this exchange. Required for real trading.",
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
