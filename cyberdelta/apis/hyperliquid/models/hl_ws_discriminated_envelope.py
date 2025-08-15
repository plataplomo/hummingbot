"""Hyperliquid discriminated union envelopes for ultra-fast validation."""

from __future__ import annotations

from typing import Literal

from pydantic import ConfigDict, Field

from cyberdelta.apis.hyperliquid.models.hl_ws_envelope import (
    HyperliquidRawWebSocketEnvelope,
    HyperliquidUserEventEnvelope,
)


class DiscriminatedHyperliquidEnvelope(HyperliquidRawWebSocketEnvelope):
    """Hyperliquid envelope with discriminator for ultra-fast validation."""

    envelope_type: Literal["hyperliquid"] = Field(
        default="hyperliquid",
        description="Discriminator field for fast union validation",
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_assignment=True,
        # Performance optimizations
        validate_default=False,
        str_strip_whitespace=True,
        use_enum_values=True,
    )


class DiscriminatedHyperliquidUserEvent(HyperliquidUserEventEnvelope):
    """Hyperliquid user event envelope with discriminator for fast validation."""

    envelope_type: Literal["hyperliquid_user_event"] = Field(
        default="hyperliquid_user_event",
        description="Discriminator field for user event validation",
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_assignment=True,
        validate_default=False,
        str_strip_whitespace=True,
        use_enum_values=True,
    )
