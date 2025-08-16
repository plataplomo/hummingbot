"""Integration layer for cross-cutting concerns between infrastructure and implementations.

This layer is allowed to import from both infrastructure (websocket) and
implementations (exchange-specific models), breaking the circular dependency
that would occur if these were in the infrastructure layer.

The files here were moved from websocket/ to fix the circular import issue.
They can be integrated and used when needed for performance optimization.
"""

from __future__ import annotations


# Note: Files in this layer are not actively used yet.
# They were moved here to break circular dependencies.
# Integration will happen when performance optimization is needed.

__all__: list[str] = []  # Will be populated when we integrate
