"""
Backpack API Account and Balance Models
--------------------------------------

Strict Pydantic models for validating account and balance responses from the Backpack Exchange API.
These models are used for boundary validation and transformation, not for internal business logic.
"""

from pydantic import BaseModel, ConfigDict, Field


class BackpackRawAccount(BaseModel):
    """
    Pydantic model for a raw account summary from `/api/v1/account` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse account payloads received from the exchange.

    Attributes:
        id (str): Unique account identifier.
        email (str): User's registered email address.
        status (str): Account status (e.g., 'active', 'suspended').
    """

    id: str = Field(..., alias="id")
    email: str = Field(..., alias="email")
    status: str = Field(..., alias="status")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawBalance(BaseModel):
    """
    Pydantic model for a raw asset balance from `/api/v1/capital` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse balance payloads received from the exchange.

    Attributes:
        asset (str): Asset/currency symbol (e.g., 'USDC', 'BTC').
        available (str): Amount available for trading (as string).
        total (str): Total balance (as string).
    """

    asset: str = Field(..., alias="asset")
    available: str = Field(..., alias="available")
    total: str = Field(..., alias="total")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
