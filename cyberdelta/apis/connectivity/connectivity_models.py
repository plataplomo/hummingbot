import re

from pydantic import AnyUrl, BaseModel, ConfigDict, Field, HttpUrl, field_validator

# Define a reasonable max length for content type strings
MAX_CONTENT_TYPE_LENGTH = 256
# Regex for typical characters in a content-type string (ASCII printables, allowing common specials)
# This is a basic sanity check, not a full MIME type grammar validation.
# Allows: letters, numbers, / . - + ; = space (space is often in params)
VALID_CONTENT_TYPE_CHARS_REGEX = re.compile(r"^[a-zA-Z0-9/.\-+=;\s]*$")


class ProcessedResponseHeaders(BaseModel):
    """
    Pydantic model to hold validated and processed HTTP response header information,
    specifically the Content-Type.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    content_type: str = Field(
        default="",
        max_length=MAX_CONTENT_TYPE_LENGTH,
        description="The lowercased Content-Type header value, or an empty string if not present.",
    )

    @field_validator("content_type")
    @classmethod
    def validate_content_type_characters(cls, v: str) -> str:
        """
        Validates that the content_type string contains acceptable characters
        and does not consist only of whitespace if not empty.
        """
        if not VALID_CONTENT_TYPE_CHARS_REGEX.fullmatch(v):
            raise ValueError("Content-Type contains invalid characters.")
        if v and not v.strip():  # If not empty, it shouldn't be just whitespace
            raise ValueError("Content-Type cannot be only whitespace.")
        return v


# --- New Config Models ---


class HttpClientConfig(BaseModel):
    """Configuration for HttpClient."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    rest_endpoint: HttpUrl
    default_request_timeout: float = Field(default=30.0, gt=0.0, le=120.0)
    max_retries: int | None = Field(default=3, ge=0, le=10)
    retry_delay_seconds: float | None = Field(default=5.0, gt=0.0, le=300.0)


class WebSocketManagerConfig(BaseModel):
    """Configuration for WebSocketManager."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    # Using AnyUrl as WebSocketUrl caused linter errors (Pydantic v1 or type resolution issue?)
    ws_url: AnyUrl
    ping_interval: float = Field(default=30.0, gt=0.0, le=60.0)
    reconnect_delay: float = Field(default=5.0, gt=0.0, le=300.0)
    max_reconnect_attempts: int = Field(default=10, ge=0, le=20)
    connection_timeout: float = Field(default=30.0, gt=0.0, le=120.0)
