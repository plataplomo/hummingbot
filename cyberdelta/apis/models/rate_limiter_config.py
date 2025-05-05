from pydantic import BaseModel, Field


class RateLimiterConfig(BaseModel):
    """
    Pydantic model for configuration and (optionally) serializable state of a token bucket
    rate limiter. This model is for config, validation, and checkpointing only. It does NOT
    include any runtime logic or async methods.

    Attributes:
        rate (float): Maximum number of requests per second.
        bucket_size (int): Maximum burst capacity (number of tokens).
        tokens (float): Current token count (optional, for checkpointing).
        last_refill (float): Timestamp of last token refill (optional, for checkpointing).
    """

    rate: float = Field(..., description="Maximum requests per second.")
    bucket_size: int = Field(..., description="Maximum burst capacity (tokens).")
    tokens: float | None = Field(
        None, description="Current token count (for checkpointing, optional)."
    )
    last_refill: float | None = Field(
        None, description="Timestamp of last refill (for checkpointing, optional)."
    )
