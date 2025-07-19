"""Hyperliquid Raw USD Transfer Response Models.

This module defines Pydantic models for validating USD transfer response payloads
from Hyperliquid's /exchange endpoint, following the exact same pattern as other
response models like HyperliquidRawOpenOrdersResponse.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field

from cyberdelta.apis.hyperliquid.models.hl_common_raw_types import (
    RawOptionalNonEmptyString1024HL,
)
from cyberdelta.utils.parsing import validate_str_field


class HyperliquidRawUsdTransferResponseData(BaseModel):
    """Raw model for successful USD transfer response data.

    Based on Hyperliquid SDK research, successful transfers return:
    {"type": "default"}
    """

    type: Annotated[
        Literal["default"],
        BeforeValidator(
            lambda x: validate_str_field(x, field_name="type", max_length=32, allow_empty=False),
        ),
    ] = Field(..., description="Response type for successful transfers, always 'default'")

    model_config = ConfigDict(extra="ignore", frozen=True)


class HyperliquidRawUsdTransferResponse(BaseModel):
    """Raw model for USD transfer responses from the /exchange endpoint.

    Follows the same pattern as other Hyperliquid exchange responses:
    - status: "ok" for success, "err" for failure
    - response: transfer data on success, error message on failure
    """

    status: Annotated[
        Literal["ok", "err"],
        BeforeValidator(
            lambda x: validate_str_field(x, field_name="status", max_length=16, allow_empty=False),
        ),
    ] = Field(..., description="Transfer operation status")

    response: HyperliquidRawUsdTransferResponseData | RawOptionalNonEmptyString1024HL | None = (
        Field(
            default=None,
            description="Transfer response data when status='ok', error when status='err'",
        )
    )

    model_config = ConfigDict(extra="forbid", frozen=True)
