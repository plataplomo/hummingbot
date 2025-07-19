"""Schema export mode enums for the API layer.

This module contains enums related to JSON schema generation and export.
"""

from enum import Enum


class SchemaExportMode(Enum):
    """Schema export mode for JSON schema generation.

    Replaces the boolean `include_examples` parameter.
    """

    MINIMAL = "minimal"
    """Generate minimal schema without examples (was include_examples=False)."""

    WITH_EXAMPLES = "with_examples"
    """Generate schema with examples (was include_examples=True)."""

    COMPREHENSIVE = "comprehensive"
    """Generate comprehensive schema with examples and descriptions."""

    API_DOCUMENTATION = "api_documentation"
    """Generate schema optimized for API documentation."""

    @property
    def should_include_examples(self) -> bool:
        """Check if examples should be included."""
        return self in {
            SchemaExportMode.WITH_EXAMPLES,
            SchemaExportMode.COMPREHENSIVE,
            SchemaExportMode.API_DOCUMENTATION,
        }


__all__ = [
    "SchemaExportMode",
]