"""Backpack discriminated union envelope for ultra-fast validation."""

from __future__ import annotations

from typing import Literal

from pydantic import ConfigDict, Field

from cyberdelta.apis.backpack.models.bp_ws_envelope import BackpackRawWebSocketEnvelope


class DiscriminatedBackpackEnvelope(BackpackRawWebSocketEnvelope):
    """Backpack envelope with discriminator for ultra-fast validation.

    Adds discriminator field to enable Pydantic's optimized union validation pathway.
    """

    envelope_type: Literal["backpack"] = Field(
        default="backpack",
        description="Discriminator field for fast union validation",
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_assignment=True,
        # Performance optimizations for high-frequency validation
        validate_default=False,  # Skip default validation for speed
        str_strip_whitespace=True,
        use_enum_values=True,
    )
