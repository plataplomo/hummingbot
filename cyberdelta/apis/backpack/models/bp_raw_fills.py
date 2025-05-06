"""
CyberDeltaEngine: Backpack API Raw Models (User Fills)
-------------------------------------------------------

Strict Pydantic models for validating the *raw* structure of Backpack Exchange API responses
related to user fills (trades) from the `/wapi/v1/history/fills` endpoint.
Adheres to the Raw Model Policy:
- Validates external contract for individual fill records.
- Validates raw data types and basic formats (non-empty, length, finite numeric, specific enums).
- Uses `model_config(extra="forbid", frozen=True)`.
- Field validators operate on raw input and return validated raw types or raise errors.
- Contains NO business logic.
"""

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
)

from cyberdelta.utils.parsing import (
    parse_datetime_utc,
    parse_decimal_value,
    validate_enum_field,
    validate_str_field,
)


# --- Core Backpack Fill Model ---
class BackpackRawFill(BaseModel):
    """
    Strict boundary Pydantic model for a single user fill (trade) object from the Backpack API
    endpoint `/wapi/v1/history/fills`. Corresponds to the `OrderFill` schema in Backpack's OpenAPI.

    This model validates the structure, raw data types, and basic formats (e.g., non-empty strings,
    valid numeric representations, boolean types, specific enum values) of individual fill records.
    It enforces immutability (`frozen=True`) and forbids extra fields (`extra='forbid').

    Field validators (`@field_validator(..., mode='before')`) operate on the raw input values.
    They ensure adherence to expected raw types and formats before Pydantic performs its
    final coercion (e.g., string to `Decimal`, string to `datetime`).

    This model adheres to the Raw Model Policy, focusing solely on validating the external API
    contract and raw data integrity for fill records.

    Attributes (after Pydantic processing):
        fee (str): The fee charged for the fill (validated as a decimal string).
        fee_symbol (str): The asset symbol in which the fee was charged.
        is_maker (bool): Indicates if the fill was for a maker order.
        order_id (str): The ID of the order associated with this fill.
        price (str): The execution price of the fill (validated as a decimal string).
        quantity (str): The executed quantity for this fill (validated as a decimal string).
        side (str): The side of the order ('Bid' or 'Ask').
        symbol (str): The trading symbol.
        timestamp (str): The execution timestamp in ISO 8601 format (e.g., "YYYY-MM-DDTHH:MM:SS.ffffffZ").
        trade_id (int): The unique ID for this trade/fill.
        client_id (str | None): Optional client-provided order ID.
    """

    fee: str = Field(...)
    fee_symbol: str = Field(..., alias="feeSymbol", max_length=32)
    is_maker: bool = Field(..., alias="isMaker")
    order_id: str = Field(..., alias="orderId", max_length=128)
    price: str = Field(...)
    quantity: str = Field(...)
    side: str = Field(..., max_length=3)  # 'Bid' or 'Ask'
    symbol: str = Field(..., max_length=64)
    timestamp: str = Field(...)  # ISO Format: "YYYY-MM-DDTHH:MM:SS.ffffffZ"
    trade_id: int = Field(..., alias="tradeId", ge=0)
    client_id: str | None = Field(None, alias="clientId", max_length=128)

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",  # Strict: No extra fields allowed
        frozen=True,  # Immutable
    )

    # --- Field Validators ---

    @field_validator("fee", "price", "quantity", mode="before")
    @classmethod
    def validate_required_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the raw input `v` for required decimal string fields (fee, price, quantity)
        is a non-empty string, has a max length of 64, and represents a finite decimal number.

        Args:
            v (object): The raw input value.
            info (ValidationInfo): Pydantic validation context.

        Returns:
            str: The validated raw string, confirmed to be a parsable finite decimal string.

        Raises:
            TypeError: If `v` is not a string.
            ValueError: If `v` is empty, exceeds max length, or not a finite decimal string.
        """
        field_name = info.field_name or "unknown_decimal_field"
        # Reuse parsing utils for consistency
        s = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        # Ensure finite
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value '{s}' must be a finite decimal.")
        return s

    @field_validator("fee_symbol", "order_id", "symbol", mode="before")
    @classmethod
    def validate_required_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the raw input `v` for required string fields (fee_symbol, order_id, symbol)
        is a non-empty string and adheres to its specified maximum length.

        Args:
            v (object): The raw input value.
            info (ValidationInfo): Pydantic validation context.

        Returns:
            str: The validated raw string value.

        Raises:
            TypeError: If `v` is not a string.
            ValueError: If `v` is empty or exceeds its defined max length.
        """
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is required for validation metadata.")
        # Fetch max_length from Field definition if possible, fallback otherwise
        model_field = cls.model_fields.get(field_name)
        max_len = getattr(
            getattr(model_field, "metadata", [None])[0],
            "max_length",
            64,  # Default fallback
        )
        return validate_str_field(v, field_name=field_name, max_length=max_len, allow_empty=False)

    @field_validator("client_id", mode="before")
    @classmethod
    def validate_optional_str(cls, v: object | None, info: ValidationInfo) -> str | None:
        """
        Validates the raw input `v` for the optional `client_id` string field.
        If provided, ensures it's a string and adheres to its max length.
        Allows an empty string at this 'before' stage; a subsequent 'after' validator
        will reject an empty string if that's the policy for non-None values.

        Args:
            v (object | None): The raw input value, which can be None.
            info (ValidationInfo): Pydantic validation context.

        Returns:
            str | None: The validated raw string value, or None if input was None.

        Raises:
            TypeError: If `v` is not a string (and not None).
            ValueError: If `v` is a string but exceeds its defined max length.
        """
        if v is None:
            return None
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is required for validation metadata.")
        # Fetch max_length from Field definition
        model_field = cls.model_fields.get(field_name)
        max_len = getattr(
            getattr(model_field, "metadata", [None])[0],
            "max_length",
            128,
        )
        # Set allow_empty=True here so the 'after' validator can specifically
        # reject the empty string case if needed.
        return validate_str_field(v, field_name=field_name, max_length=max_len, allow_empty=True)

    @field_validator("client_id", mode="after")
    @classmethod
    def check_client_id_not_empty_str(cls, v: str | None) -> str | None:
        """
        Ensures that if `clientId` is provided (i.e., not None after 'before' validation),
        it is not an empty or whitespace-only string.

        This runs *after* Pydantic assigns None or the (potentially empty) validated string
        from the `mode='before'` validator.

        Args:
            v (str | None): The value of `clientId` after 'before' validation and assignment.

        Returns:
            str | None: The validated `clientId` (will not be an empty/whitespace string if not None).

        Raises:
            ValueError: If `v` is a string but consists only of whitespace or is empty.
        """
        # This runs after Pydantic assigns None or the validated string from the 'before' validator.
        if v is not None and not v.strip():
            # This case should theoretically be caught by allow_empty=False
            # in the 'before' validator, but this provides an explicit,
            # redundant check as mandated.
            raise ValueError("clientId cannot be an empty or whitespace-only string if provided.")
        return v

    @field_validator("is_maker", mode="before")
    @classmethod
    def validate_bool(cls, v: object, info: ValidationInfo) -> bool:
        """
        Validates that the raw input `v` for the boolean `is_maker` field is a boolean.

        Args:
            v (object): The raw input value.
            info (ValidationInfo): Pydantic validation context.

        Returns:
            bool: The validated boolean value.

        Raises:
            ValueError: If `v` is not a boolean (Pydantic uses ValueError for type mismatches
                        at this stage if strict coercion is not enabled globally for bools).
        """
        field_name = info.field_name or "unknown_bool_field"
        if not isinstance(v, bool):
            # Use ValueError for Pydantic compatibility
            raise ValueError(f"{field_name}: Must be a boolean, got {type(v).__name__}")
        return v

    @field_validator("side", mode="before")
    @classmethod
    def validate_side_enum(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the raw input `v` for the `side` field is one of the allowed
        enum values ('Bid' or 'Ask').

        Args:
            v (object): The raw input value.
            info (ValidationInfo): Pydantic validation context.

        Returns:
            str: The validated side string ('Bid' or 'Ask').

        Raises:
            TypeError: If `v` is not a string.
            ValueError: If `v` is not 'Bid' or 'Ask'.
        """
        field_name = info.field_name or "unknown_enum_field"
        return validate_enum_field(v, allowed={"Bid", "Ask"}, field_name=field_name)

    @field_validator("trade_id", mode="before")
    @classmethod
    def validate_non_negative_int(cls, v: object, info: ValidationInfo) -> int:
        """
        Validates that the raw input `v` for the `trade_id` field is a non-negative integer.
        Allows integer strings.

        Args:
            v (object): The raw input value.
            info (ValidationInfo): Pydantic validation context.

        Returns:
            int: The validated non-negative integer value.

        Raises:
            TypeError: If `v` is not an integer or a string representing an integer.
            ValueError: If `v` is negative.
        """
        field_name = info.field_name or "unknown_int_field"
        if isinstance(v, int):
            v_int = v
        elif isinstance(v, str) and v.isdigit():
            v_int = int(v)
        else:
            raise TypeError(f"{field_name}: Must be an integer, got {type(v).__name__}")

        # Check non-negativity using the Field constraint (ge=0) if possible,
        # otherwise explicitly check here.
        min_val = 0
        if info.field_name:
            model_field = cls.model_fields.get(info.field_name)
            if model_field:
                min_val = getattr(
                    getattr(model_field, "metadata", [None])[0],
                    "ge",
                    0,
                )

        if v_int < min_val:
            raise ValueError(f"{field_name}: Must be >= {min_val}, got {v_int}")
        return v_int

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_iso_timestamp_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the raw input `v` for the `timestamp` field is a non-empty string
        representing a valid ISO 8601 datetime, parsable into a UTC datetime object.

        Args:
            v (object): The raw input value.
            info (ValidationInfo): Pydantic validation context.

        Returns:
            str: The validated raw ISO 8601 datetime string.

        Raises:
            TypeError: If `v` is not a string.
            ValueError: If `v` is empty or not a valid ISO 8601 datetime string parsable to UTC.
        """
        field_name = info.field_name or "timestamp"

        # First, validate it's a non-empty string. Backpack API spec suggests
        # format "YYYY-MM-DDTHH:MM:SS.ffffffZ". Max length can be around 30-40.
        # Example: "2023-04-12T14:30:00.123456Z" is 27 chars.
        # Let's use a generous max_length like 64 for safety, unless a stricter one is known.
        raw_ts_str = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)

        # Then, attempt to parse it using the utility function to ensure it's a valid UTC datetime.
        try:
            dt_obj = parse_datetime_utc(raw_ts_str, field_name=field_name)
            if (
                dt_obj is None
            ):  # parse_datetime_utc returns None if input is None, but here raw_ts_str is not None.
                # This case should ideally not be hit if validate_str_field ensures non-empty string.
                raise ValueError(
                    f"{field_name}: Successfully validated as string '{raw_ts_str}', "
                    f"but parse_datetime_utc returned None unexpectedly."
                )
        except ValueError as e:
            # Re-raise with context if parse_datetime_utc fails
            raise ValueError(
                f"{field_name}: Invalid ISO timestamp string '{raw_ts_str}'. Reason: {e}"
            ) from e

        # Return the original validated string, as Pydantic will handle final conversion to datetime
        return raw_ts_str
