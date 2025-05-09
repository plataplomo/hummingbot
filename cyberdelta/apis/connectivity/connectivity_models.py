import re

from pydantic import BaseModel, Field, field_validator

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
