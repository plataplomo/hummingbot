"""Secure JSON parsing utilities for DoS protection.

This module provides centralized JSON security functions to protect against
JSON bomb attacks, deeply nested objects, and algorithmic complexity exploits.
"""

from __future__ import annotations

import json
from typing import Any


# JSON security constants
MAX_JSON_SIZE = 1024 * 1024  # 1MB limit
MAX_JSON_DEPTH = 50  # Maximum nesting depth
MAX_JSON_ITEMS = 10000  # Maximum number of items/keys


def secure_json_loads(
    data: str | bytes,
    max_size: int = MAX_JSON_SIZE,
    max_depth: int = MAX_JSON_DEPTH,
    max_items: int = MAX_JSON_ITEMS,
) -> dict[str, Any] | list[Any] | str | int | float | bool | None:
    """Secure JSON parsing with comprehensive DoS protection.

    Args:
        data: JSON data to parse
        max_size: Maximum size in bytes (default 1MB)
        max_depth: Maximum nesting depth (default 50)
        max_items: Maximum number of items/keys (default 10000)

    Returns:
        Parsed JSON data

    Raises:
        ValueError: If data exceeds security limits
        json.JSONDecodeError: If JSON is invalid
    """
    # Size validation
    data_size = len(data)
    if data_size > max_size:
        msg = f"JSON payload size {data_size} exceeds maximum {max_size} bytes"
        raise ValueError(msg)

    # Parse JSON
    parsed = json.loads(data)

    # Depth and complexity validation
    _validate_json_structure(parsed, max_depth, max_items)

    return parsed  # type: ignore[no-any-return]


def _validate_json_structure(
    obj: dict[str, Any] | list[Any] | str | float | bool | None,
    max_depth: int,
    max_items: int,
    current_depth: int = 0,
) -> None:
    """Validate JSON structure for DoS protection.

    Recursively validates JSON structure to prevent:
    - Excessive nesting depth (stack overflow attacks)
    - Excessive number of items/keys (memory exhaustion attacks)
    - Algorithmic complexity exploits

    Args:
        obj: JSON object to validate
        max_depth: Maximum allowed nesting depth
        max_items: Maximum allowed items/keys per container
        current_depth: Current nesting depth (internal use)

    Raises:
        ValueError: If structure exceeds security limits
    """
    if current_depth > max_depth:
        msg = f"JSON nesting depth {current_depth} exceeds maximum {max_depth}"
        raise ValueError(msg)

    if isinstance(obj, dict):
        if len(obj) > max_items:
            msg = f"JSON object has {len(obj)} keys, exceeds maximum {max_items}"
            raise ValueError(msg)
        for value in obj.values():
            _validate_json_structure(value, max_depth, max_items, current_depth + 1)

    elif isinstance(obj, list):
        if len(obj) > max_items:
            msg = f"JSON array has {len(obj)} items, exceeds maximum {max_items}"
            raise ValueError(msg)
        for item in obj:
            _validate_json_structure(item, max_depth, max_items, current_depth + 1)
