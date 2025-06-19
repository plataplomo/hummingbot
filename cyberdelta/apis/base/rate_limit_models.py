"""Rate limiting models for exchange API rate management.

This module provides Pydantic models for rate limiting context and data structures,
replacing dict usage with type-safe models.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class RateLimitRequestContext(BaseModel):
    """Context information for rate limiting decisions.

    This model replaces the dict[str, Any] request_context used in rate limiting.
    """

    exchange_name: str = Field(..., description="Name of the exchange")
    method: str = Field(..., description="HTTP method (GET, POST, etc.)")
    endpoint: str = Field(..., description="API endpoint path")
    action_payload: dict[str, Any] | None = Field(None, description="Request payload data")
    request_weight: int = Field(1, description="Weight of this request for rate limiting")
    endpoint_group: str | None = Field(None, description="Logical group for the endpoint")

    model_config = ConfigDict(extra="forbid", frozen=True)
