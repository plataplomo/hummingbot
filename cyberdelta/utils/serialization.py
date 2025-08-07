"""Serialization utilities for CyberDeltaEngine.

This module provides JSON encoding and decoding functionality using orjson
for high performance and native support for Decimal, datetime, and numpy types.
"""

from decimal import Decimal
from typing import TypeVar, cast, overload

import orjson
from pydantic import BaseModel


# Type alias for JSON-compatible values
type JSONValue = str | int | float | bool | dict[str, "JSONValue"] | list["JSONValue"] | None

# Generic type for serializable objects
T = TypeVar("T")

# Type alias for objects that can be serialized to JSON
type SerializableType = (
    JSONValue
    | BaseModel
    | Decimal
    | bytes
    | dict[str, "SerializableType"]
    | list["SerializableType"]
    | tuple["SerializableType", ...]
)


@overload
def dumps_json(
    obj: BaseModel,
    *,
    indent: bool = False,
    sort_keys: bool = False,
) -> str: ...


@overload
def dumps_json(
    obj: SerializableType,
    *,
    indent: bool = False,
    sort_keys: bool = False,
) -> str: ...


def dumps_json(
    obj: SerializableType,
    *,
    indent: bool = False,
    sort_keys: bool = False,
) -> str:
    """Serialize obj to a JSON formatted string using orjson.

    Args:
        obj: Object to serialize (Pydantic models, dicts, lists, primitives, Decimal)
        indent: Whether to indent the output (2 spaces)
        sort_keys: Whether to sort dictionary keys

    Returns:
        JSON formatted string

    Note:
        orjson natively handles:
        - Decimal (preserves precision)
        - datetime/date (ISO format)
        - numpy arrays
        - UUID
        - Pydantic models (via model_dump)
    """
    options = 0

    if indent:
        options |= orjson.OPT_INDENT_2
    if sort_keys:
        options |= orjson.OPT_SORT_KEYS

    # Handle Pydantic models
    if isinstance(obj, BaseModel):
        obj = obj.model_dump(mode="json", by_alias=True, exclude_none=True)

    # orjson returns bytes, decode to string
    return orjson.dumps(obj, option=options).decode("utf-8")


def dumps_json_bytes(
    obj: SerializableType,
    *,
    indent: bool = False,
    sort_keys: bool = False,
) -> bytes:
    """Serialize obj to JSON bytes using orjson.

    This is more efficient than dumps_json when you need bytes,
    as it avoids the decode step.

    Args:
        obj: Object to serialize (Pydantic models, dicts, lists, primitives, Decimal)
        indent: Whether to indent the output (2 spaces)
        sort_keys: Whether to sort dictionary keys

    Returns:
        JSON formatted bytes
    """
    options = 0

    if indent:
        options |= orjson.OPT_INDENT_2
    if sort_keys:
        options |= orjson.OPT_SORT_KEYS

    # Handle Pydantic models
    if isinstance(obj, BaseModel):
        obj = obj.model_dump(mode="json", by_alias=True, exclude_none=True)

    return orjson.dumps(obj, option=options)


def loads_json(json_str: str | bytes) -> JSONValue:
    """Deserialize JSON string or bytes to a Python object.

    Args:
        json_str: JSON string or bytes

    Returns:
        Deserialized Python object (dict, list, str, int, float, bool, or None)
    """
    # orjson.loads can raise orjson.JSONDecodeError which is handled by caller
    # Cast is safe: orjson.loads always returns valid JSON types
    result = orjson.loads(json_str)
    return cast(JSONValue, result)


# Backward compatibility aliases
def serialize_to_json(obj: SerializableType, **kwargs: bool) -> str:
    """Legacy function name for dumps_json.

    Args:
        obj: Object to serialize
        **kwargs: Additional keyword arguments (indent, sort_keys)

    Returns:
        JSON formatted string
    """
    return dumps_json(obj, **kwargs)


def deserialize_from_json(json_str: str | bytes) -> JSONValue:
    """Legacy function name for loads_json.

    Args:
        json_str: JSON string or bytes to deserialize

    Returns:
        Deserialized Python object
    """
    return loads_json(json_str)
