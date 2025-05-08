from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

# cyberdelta.utils.parsing is no longer directly needed here, but common_raw_types needs it.
# from cyberdelta.utils.parsing import validate_str_field
from .common_raw_types import (
    RawDefaultString,
    RawEthereumAddressStr,
    RawNonNegativeInt,
)


class HyperliquidRawOrderStatusRequestPayload(BaseModel):
    """
    Request payload for the 'orderStatus' info type.
    """

    type: Annotated[Literal["orderStatus"], RawDefaultString] = Field("orderStatus")
    user: RawEthereumAddressStr
    oid: RawNonNegativeInt

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Removed old field validators:
    # - validate_type_literal
    # - validate_user_address
    # - validate_oid
