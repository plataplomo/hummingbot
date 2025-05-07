from pydantic import BaseModel, ConfigDict, Field


class EndpointRateConfig(BaseModel):
    """
    Configuration for a specific API endpoint's rate limiter.
    """

    rate: float = Field(..., description="Maximum requests per second for this endpoint.")
    bucket_size: int = Field(..., description="Maximum burst capacity (tokens) for this endpoint.")
    model_config = ConfigDict(extra="forbid")


class RateLimiterConfig(BaseModel):
    """
    Pydantic model for the overall rate limiting configuration structure.
    It defines default rates and allows for overriding these defaults for specific endpoints.
    The fields 'tokens' and 'last_refill' are for potential checkpointing of a
    default/global limiter's state, not typically part of the initial service config.
    """

    default_rate: float = Field(..., description="Default maximum requests per second.")
    default_bucket_size: int = Field(..., description="Default maximum burst capacity (tokens).")

    endpoints: dict[str, EndpointRateConfig] | None = Field(
        default=None, description="Optional configurations for specific endpoints."
    )

    tokens: float | None = Field(
        default=None, description="Current token count (for checkpointing, optional)."
    )
    last_refill: float | None = Field(
        default=None, description="Timestamp of last refill (for checkpointing, optional)."
    )

    # Fields from the original model that seem to describe a single limiter's parameters.
    # If RateLimiterConfig is for the *service*, these might be redundant if defaults are present.
    # Or they could be a base template if no defaults are given.
    # Clarification: Assuming default_rate/default_bucket_size are the primary defaults.
    # The original `rate` and `bucket_size` are removed to avoid confusion with `default_rate`
    # and `default_bucket_size`. If a base rate/bucket (not default) is needed,
    # it should be reconsidered. For now, focusing on the structure for RateLimiterService.
    # rate: float = Field(..., description="Maximum requests per second.")
    # bucket_size: int = Field(..., description="Maximum burst capacity (tokens).")

    model_config = ConfigDict(extra="forbid")
