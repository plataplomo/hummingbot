"""Exchange API configuration model."""
from typing import Any

from pydantic import BaseModel, Field


class ExchangeAPIConfig(BaseModel):
    """Pydantic model for configuration of an ExchangeAPI instance.
    This model is for config/validation only, not for runtime logic or state.

    Attributes:
        rest_endpoint (str): Base URL for REST API.
        ws_endpoint (str): Base URL for WebSocket API.
        api_key (str): API key for authentication.
        api_secret (str): API secret for authentication.
        rate_limits (dict | None): Optional rate limit configuration (raw dict or validated model).

    """

    rest_endpoint: str = Field(..., description="Base URL for REST API.")
    ws_endpoint: str = Field(..., description="Base URL for WebSocket API.")
    api_key: str = Field(..., description="API key for authentication.")
    api_secret: str = Field(..., description="API secret for authentication.")
    rate_limits: dict[str, Any] | None = Field(
        None,
        description="Optional rate limit configuration (raw dict or validated model).",
    )
