"""Security configuration models for WebSocket processing.

This module contains Pydantic models for security configuration
in the WebSocket system.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


# Security configuration constants
DEFAULT_MAX_MESSAGE_SIZE_BYTES = 1024 * 1024  # 1MB
DEFAULT_MAX_NESTING_DEPTH = 10
DEFAULT_MAX_STRING_LENGTH = 10000
DEFAULT_MAX_ARRAY_LENGTH = 1000
DEFAULT_MAX_OBJECT_KEYS = 100


class SecurityConfig(BaseModel):
    """Security configuration for WebSocket processing.

    This configuration defines limits and controls to protect against
    various attack vectors in WebSocket message processing.
    """

    max_message_size_bytes: int = Field(
        default=DEFAULT_MAX_MESSAGE_SIZE_BYTES,
        gt=0,
        le=10 * 1024 * 1024,  # Max 10MB
        description="Maximum message size in bytes to prevent memory exhaustion attacks",
    )
    max_nesting_depth: int = Field(
        default=DEFAULT_MAX_NESTING_DEPTH,
        gt=0,
        le=50,
        description="Maximum object nesting depth to prevent stack overflow attacks",
    )
    max_string_length: int = Field(
        default=DEFAULT_MAX_STRING_LENGTH,
        gt=0,
        le=100000,
        description="Maximum string length to prevent memory exhaustion",
    )
    max_array_length: int = Field(
        default=DEFAULT_MAX_ARRAY_LENGTH,
        gt=0,
        le=10000,
        description="Maximum array length to prevent memory exhaustion",
    )
    max_object_keys: int = Field(
        default=DEFAULT_MAX_OBJECT_KEYS,
        gt=0,
        le=1000,
        description="Maximum number of keys in an object to prevent resource exhaustion",
    )
    enable_content_filtering: bool = Field(
        default=True,
        description="Enable content filtering for malicious patterns",
    )
    blocked_patterns: list[str] = Field(
        default_factory=list,
        description="List of regex patterns to block in string content",
    )
    enable_size_validation: bool = Field(
        default=True,
        description="Enable message size validation",
    )
    enable_depth_validation: bool = Field(
        default=True,
        description="Enable nesting depth validation",
    )
    enable_structure_validation: bool = Field(
        default=True,
        description="Enable data structure validation",
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_assignment=True,
    )
