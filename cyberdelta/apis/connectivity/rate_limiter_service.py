import asyncio
from typing import Any

from pydantic import BaseModel, ConfigDict

from cyberdelta.apis.models.rate_limiter_config import RateLimiterConfig
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)

SERVICE_FALLBACK_RATE = 1.0
SERVICE_FALLBACK_BUCKET_SIZE = 1


class RateLimiterService(BaseModel):
    """
    Manages rate limiters for API requests.
    """

    default_limiter: TokenBucketRateLimiterRuntime
    endpoint_limiters: dict[str, TokenBucketRateLimiterRuntime]
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True, extra="forbid")

    def __init__(
        self,
        exchange_name: str,
        config: dict[str, Any],
        loop: asyncio.AbstractEventLoop
        | None = None,  # loop is not used by TokenBucketRateLimiterRuntime
    ) -> None:
        """
        Initialize the RateLimiterService.

        Args:
            exchange_name: Name of the exchange for logging context.
            config: Dictionary containing rate limit configurations.
                    Expected structure under 'rate_limits':
                        default_rate: float
                        default_bucket_size: int
                        endpoints: Optional[Dict[str, EndpointRateConfig]]
            loop: The asyncio event loop (optional, deprecated).
        """
        parsed_rate_config: RateLimiterConfig
        try:
            rate_limits_data = config.get("rate_limits", {})
            if not isinstance(
                rate_limits_data, dict
            ):  # Should not happen if config.get default is {}
                logger.warning(
                    f"[{exchange_name}] 'rate_limits' in config is not a dictionary. "
                    f"Attempting to use empty config."
                )
                rate_limits_data = {}
            parsed_rate_config = RateLimiterConfig.model_validate(rate_limits_data)
        except Exception as e:  # Includes Pydantic ValidationError
            logger.error(
                f"[{exchange_name}] Failed to validate 'rate_limits' config: {e}. "
                f"Using service fallbacks: rate={SERVICE_FALLBACK_RATE}, "
                f"bucket={SERVICE_FALLBACK_BUCKET_SIZE}."
            )

            parsed_rate_config = RateLimiterConfig(
                default_rate=SERVICE_FALLBACK_RATE,
                default_bucket_size=SERVICE_FALLBACK_BUCKET_SIZE,
                endpoints=None,  # No specific endpoints in this fallback case
            )

        # Initialize default_limiter using values from the parsed config
        # (which might be service fallbacks if parsing failed)
        default_limiter = TokenBucketRateLimiterRuntime(
            rate=parsed_rate_config.default_rate,
            bucket_size=parsed_rate_config.default_bucket_size,
        )

        initialized_endpoint_limiters: dict[str, TokenBucketRateLimiterRuntime] = {}

        if parsed_rate_config.endpoints:
            for endpoint_pattern, endpoint_cfg in parsed_rate_config.endpoints.items():
                if endpoint_cfg:  # endpoint_cfg is EndpointRateConfig
                    try:
                        initialized_endpoint_limiters[endpoint_pattern] = (
                            TokenBucketRateLimiterRuntime(
                                rate=endpoint_cfg.rate, bucket_size=endpoint_cfg.bucket_size
                            )
                        )
                    except Exception as e:  # Catch potential errors during limiter creation
                        logger.warning(
                            f'[{exchange_name}] Error creating limiter for endpoint "'
                            f'{endpoint_pattern}": {e}. Using default limiter values '
                            f"(rate={parsed_rate_config.default_rate}, "
                            f"bucket={parsed_rate_config.default_bucket_size}) for this endpoint."
                        )
                        initialized_endpoint_limiters[endpoint_pattern] = (
                            TokenBucketRateLimiterRuntime(
                                rate=parsed_rate_config.default_rate,
                                bucket_size=parsed_rate_config.default_bucket_size,
                            )
                        )
                else:
                    logger.warning(
                        f'[{exchange_name}] Missing configuration for endpoint "'
                        f'{endpoint_pattern}". Using default limiter values for this endpoint.'
                    )
                    initialized_endpoint_limiters[endpoint_pattern] = TokenBucketRateLimiterRuntime(
                        rate=parsed_rate_config.default_rate,
                        bucket_size=parsed_rate_config.default_bucket_size,
                    )

        super().__init__(
            default_limiter=default_limiter, endpoint_limiters=initialized_endpoint_limiters
        )

    def get_limiter(self, method: str, path: str) -> TokenBucketRateLimiterRuntime:
        """
        Get the appropriate rate limiter for the given HTTP method and path.

        Args:
            method: HTTP method (e.g., 'GET', 'POST').
            path: API endpoint path.

        Returns:
            The specific TokenBucketRateLimiterRuntime for the endpoint, or the default.
        """
        endpoint_key_method_path = f"{method.upper()}:{path}"
        endpoint_key_path = path

        if endpoint_key_method_path in self.endpoint_limiters:
            return self.endpoint_limiters[endpoint_key_method_path]
        if endpoint_key_path in self.endpoint_limiters:
            return self.endpoint_limiters[endpoint_key_path]
        return self.default_limiter
