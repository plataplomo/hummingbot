"""Router metadata models for WebSocket error handling.

This module contains Pydantic models for router-specific error metadata
in the WebSocket system.
"""

from __future__ import annotations

import time

from pydantic import BaseModel, Field


class RouterErrorMetadata(BaseModel):
    """Router-specific error metadata for enhanced error context.

    Extends the base error metadata with router-specific information
    like routing keys, processor availability, and envelope details.
    """

    model_config = {
        "frozen": True,
        "extra": "forbid",
    }

    # Router identification
    router_type: str
    exchange_name: str
    connection_id: str

    # Routing information
    routing_key: str | None = None
    available_processors: list[str] | None = None
    available_handlers: list[str] | None = None

    # Message information
    envelope_type: str | None = None
    message_keys: list[str] | None = None
    message_size_bytes: int | None = None

    # Error stage information
    error_stage: str  # "envelope_validation", "routing", "processor_lookup", etc.

    # Performance data
    processing_start_time_ms: int | None = None
    error_timestamp_ms: int = Field(default_factory=lambda: int(time.time() * 1000))
