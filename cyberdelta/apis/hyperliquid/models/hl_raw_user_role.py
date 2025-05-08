"""
CyberDeltaEngine: Hyperliquid API Raw Models (User Role)
-------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'userRole' info endpoint.
Validates the raw structure only.
Never use for internal business logic.
"""

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
)

from cyberdelta.utils.parsing import (
    validate_enum_field,
    validate_str_field,
)


class HyperliquidRawUserRoleData(BaseModel):
    """
    Raw boundary model for the optional 'data' field in the userRole response.
    Structure varies based on the role.
    """

    user: str | None = Field(None, alias="user")  # For agent role
    master: str | None = Field(None, alias="master")  # For subAccount role

    model_config = ConfigDict(populate_by_name=True, extra="allow", frozen=True)
    # Allow extra fields as structure might vary for other roles

    @field_validator("user", "master", mode="before")
    @classmethod
    def validate_optional_address(cls, v: object, info: ValidationInfo) -> str | None:
        field_name = info.field_name or "optional_address_field"
        if v is None:
            return None
        s = validate_str_field(v, field_name=field_name, max_length=42)
        if len(s) != 42:
            raise ValueError(f"{field_name}: Expected length 42, got {len(s)}")
        if not s.startswith("0x"):
            raise ValueError(f"{field_name}: Must start with 0x")
        return s


class HyperliquidRawUserRoleResponse(BaseModel):
    """
    Raw boundary model for the user role response.
    """

    role: str = Field(..., alias="role")
    data: HyperliquidRawUserRoleData | None = Field(None, alias="data")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("role", mode="before")
    @classmethod
    def validate_role_enum(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "role"
        # Validate against known roles from docs
        allowed_roles = {"missing", "user", "agent", "vault", "subAccount"}
        s = validate_str_field(v, field_name=field_name, max_length=32)
        return validate_enum_field(s, allowed=allowed_roles, field_name=field_name)

    # 'data' field validation happens via the nested HyperliquidRawUserRoleData model
