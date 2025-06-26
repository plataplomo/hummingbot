"""Signing-related validators and serializers for Hyperliquid models.

This module provides common validators and serializers used across Hyperliquid
models to prepare data for EIP-712 signing. These validators ensure consistent
formatting, address normalization, and field ordering required by the signing process.
"""

from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator, model_serializer
from pydantic_core.core_schema import SerializationInfo

from cyberdelta.utils.typing import is_dict_str_any, is_list_any


def normalize_ethereum_address(address: str) -> str:
    """Normalize an Ethereum address to lowercase for consistent signing.

    Args:
        address: Ethereum address string

    Returns:
        Lowercase Ethereum address
    """
    if len(address) == 42 and address.lower().startswith("0x"):
        return address.lower()
    return address


class EthereumAddressNormalizer:
    """Mixin class that provides Ethereum address normalization validators."""

    @field_validator(
        "destination",
        "address",
        "from_address",
        "to_address",
        mode="before",
        check_fields=False,
    )
    @classmethod
    def normalize_address_fields(cls, v: str | Any) -> str | Any:  # noqa: ANN401
        """Normalize Ethereum address fields to lowercase."""
        if isinstance(v, str):
            return normalize_ethereum_address(v)
        return v


class SigningPayloadSerializer:
    """Mixin class that provides serialization for signing payloads."""

    @model_serializer(mode="wrap")
    def serialize_for_signing(
        self,
        serializer: Callable[[Any], Any],
        info: SerializationInfo,
    ) -> dict[str, Any]:
        """Serialize model for signing, removing None values and ensuring proper order.

        This serializer:
        1. Respects the by_alias setting from model_dump
        2. Excludes None/null values
        3. Maintains field ordering required by signing
        """
        # Get the default serialization with the same settings as model_dump
        # The info object contains serialization context including by_alias setting
        data = serializer(self)

        # Clean the data for signing
        return self._clean_for_signing(data)

    def _clean_for_signing(self, data: dict[str, Any]) -> dict[str, Any]:
        """Recursively clean data for signing."""
        return self._clean_dict_recursive(data)

    def _clean_dict_recursive(self, data: dict[str, Any]) -> dict[str, Any]:
        """Helper method to recursively clean dictionary data."""
        cleaned: dict[str, Any] = {}

        for key, value in data.items():
            if value is None:
                continue

            cleaned_value = self._clean_value(value)
            if cleaned_value is not None:
                cleaned[key] = cleaned_value

        return cleaned

    def _clean_value(self, value: Any) -> Any:  # noqa: ANN401
        """Clean a single value for signing."""
        if is_dict_str_any(value):
            # value is now properly typed as dict[str, Any] due to TypeGuard
            cleaned_dict = self._clean_dict_recursive(value)
            return cleaned_dict or None
        if is_list_any(value):
            # value is now properly typed as list[Any] due to TypeGuard
            return self._clean_list(value)
        return value

    def _clean_list(self, lst: list[Any]) -> list[Any] | None:
        """Clean a list for signing."""
        cleaned_list: list[Any] = []
        for item in lst:
            if is_dict_str_any(item):
                # item is now properly typed as dict[str, Any] due to TypeGuard
                cleaned_item = self._clean_dict_recursive(item)
                if cleaned_item:
                    cleaned_list.append(cleaned_item)
            elif item is not None:
                cleaned_list.append(item)
        return cleaned_list or None


class OrderTypeCleanerMixin:
    """Mixin for cleaning order type structures."""

    @field_validator("order_type_details", "t", mode="after", check_fields=False)
    @classmethod
    def clean_order_type(cls, v: dict[str, Any] | object) -> dict[str, Any] | object:
        """Clean order type structure by removing null limit/market fields.

        Transforms: {"limit": {...}, "market": null} -> {"limit": {...}}
        """
        if is_dict_str_any(v) and "limit" in v and "market" in v:
            # v is now properly typed as dict[str, Any] due to TypeGuard
            if v["limit"] is None and v["market"] is not None:
                return {"market": v["market"]}
            if v["market"] is None and v["limit"] is not None:
                return {"limit": v["limit"]}
        return v


class GenericSigningPayload(BaseModel, SigningPayloadSerializer):
    """Generic model for handling arbitrary signing payloads.

    This model can accept any dict structure and properly serialize it
    for signing. Used as a fallback when the authenticator receives
    raw dict data instead of typed Pydantic models.
    """

    model_config = ConfigDict(extra="allow", frozen=True)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GenericSigningPayload":
        """Create a GenericSigningPayload from a dictionary.

        This handles the conversion of arbitrary dict structures
        into a Pydantic model that can be properly serialized.
        """
        return cls(**data)
