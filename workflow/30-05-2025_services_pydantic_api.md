

**Prompt for AI Coder (Angel): Refactor `place_order` Service Method Inputs with Pydantic Model**

**Project:** CyberDeltaEngine
**Context:**
We are enhancing the robustness and clarity of our internal service layer interfaces. The `place_order` method, due to its numerous parameters and inter-dependent validation logic, is a prime candidate for refactoring to use a Pydantic model for its arguments. This will centralize input validation and make the method signature cleaner.

**Goal:**
1.  Define a `PlaceOrderArgs` Pydantic model in `cyberdelta/apis/models/service_args_models.py` to encapsulate all arguments for placing an order.
2.  Refactor the abstract `ExchangeAPI.place_order` method to accept this `PlaceOrderArgs` model.
3.  Refactor the concrete implementations of `place_order` in `BackpackAPI`, `HyperliquidAPI`, and their respective trading services (`BackpackTradingService`, `HyperliquidTradingService`) to use this `PlaceOrderArgs` model.
4.  Update all call sites of `place_order` throughout the codebase to instantiate and pass `PlaceOrderArgs`.

**Why:**
*   **Centralized Validation:** Consolidates input validation logic (type checks, value constraints, inter-dependencies) into the `PlaceOrderArgs` Pydantic model.
*   **Improved Clarity:** Method signatures become `method(self, args: PlaceOrderArgs)`.
*   **Reduced Boilerplate:** Less repetitive validation code within each service method implementation.
*   **Testability:** The `PlaceOrderArgs` model can be unit-tested independently for its validation logic.
*   **Maintainability:** Easier to add or modify parameters related to order placement in the future.

**What to do (Step-by-Step for `place_order`):**

1.  **Define `PlaceOrderArgs` Pydantic Model:**
    *   **File:** `cyberdelta/apis/models/service_args_models.py` (Create this file if it doesn't exist. Ensure it's added to the `cyberdelta/apis/models/__init__.py` `__all__` list).
    *   **Imports needed in `service_args_models.py`:**
        ```python
        from decimal import Decimal
        from pydantic import BaseModel, Field, model_validator, field_validator, ValidationInfo, ConfigDict
        from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
        from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field
        ```
    *   **Model Definition (`PlaceOrderArgs`):**
        ```python
        class PlaceOrderArgs(BaseModel):
            model_config = ConfigDict(extra="forbid", validate_assignment=True)

            symbol: str
            side: OrderSide
            order_type: OrderType
            quantity: Decimal = Field(gt=Decimal("0"))
            time_in_force: TimeInForce
            price: Decimal | None = Field(default=None, gt=Decimal("0"))
            stop_price: Decimal | None = Field(default=None, gt=Decimal("0"))
            client_order_id: str | None = Field(default=None)
            reduce_only: bool = Field(default=False)
            post_only: bool = Field(default=False)

            @field_validator("symbol", mode="before")
            @classmethod
            def validate_symbol_str(cls, v: Any, info: ValidationInfo) -> str: # Use Any for raw input
                # field_name is guaranteed by Pydantic to be correct here.
                return validate_str_field(v, field_name=str(info.field_name), max_length=64, allow_empty=False)

            @field_validator("client_order_id", mode="before")
            @classmethod
            def validate_client_order_id_str(cls, v: Any, info: ValidationInfo) -> str | None: # Use Any for raw input
                if v is None:
                    return None
                return validate_str_field(v, field_name=str(info.field_name), max_length=64, allow_empty=False) # Assuming non-empty if provided

            @field_validator("quantity", "price", "stop_price", mode="before")
            @classmethod
            def parse_decimal_fields(cls, v: Any, info: ValidationInfo) -> Decimal | None: # Use Any for raw input
                field_name = str(info.field_name)
                is_required = field_name == "quantity"
                parsed = parse_decimal_value(v, field_name=field_name, allow_none=not is_required)
                if parsed is None and is_required:
                    raise ValueError(f"Field '{field_name}' is required and cannot be None or invalid.")
                if parsed is not None:
                    if not parsed.is_finite():
                        raise ValueError(f"Field '{field_name}' must be a finite decimal, got {v}.")
                    # Positivity (gt=0) is handled by Field constraint AFTER this validator.
                    # If Field constraint was not present, we'd add: if parsed <= 0 and (field_name == 'quantity' or (field_name in ['price', 'stop_price'] and parsed is not None)): raise ValueError(...)
                return parsed

            @model_validator(mode="after")
            def check_parameter_dependencies(self) -> "PlaceOrderArgs":
                if self.order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT] and (self.price is None): # Removed price <= 0 check as Field gt=0 handles it
                    raise ValueError(f"A positive price is required for {self.order_type.value} orders.")
                if self.order_type in [OrderType.STOP_MARKET, OrderType.STOP_LIMIT] and (self.stop_price is None): # Removed stop_price <= 0 check
                    raise ValueError(f"A positive stop_price is required for {self.order_type.value} orders.")
                if self.post_only and self.order_type != OrderType.LIMIT:
                    raise ValueError("Post-only (post_only=True) is only applicable to LIMIT orders.")
                # Specific client_order_id format checks (e.g., Backpack int conversion) should ideally
                # be handled within the exchange-specific RequestBuilder or service, not in this generic Args model.
                # If a truly universal format constraint existed, it could be here.
                return self
        ```

2.  **Refactor `ExchangeAPI.place_order` (Abstract Method):**
    *   **File:** `cyberdelta/apis/base/exchange_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import PlaceOrderArgs` (adjust path if needed)
    *   **Change Signature:**
        ```python
        @abstractmethod
        async def place_order(self, args: PlaceOrderArgs) -> Order:
            raise NotImplementedError
        ```
        *Remove the old multi-parameter signature.*

3.  **Refactor `BackpackTradingService.place_order`:**
    *   **File:** `cyberdelta/apis/backpack/services/bp_trading_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import PlaceOrderArgs`
    *   **Change Signature:** `async def place_order(self, args: PlaceOrderArgs) -> Order:`
    *   **Logic:**
        *   Remove all existing individual parameter validation code at the start of the method.
        *   Access parameters via `args.symbol`, `args.side`, `args.quantity`, etc.
        *   Update the call to `self._request_builder.build_place_order_payload` to use fields from `args`.

4.  **Refactor `HyperliquidTradingService.place_order`:**
    *   **File:** `cyberdelta/apis/hyperliquid/services/hl_trading_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import PlaceOrderArgs`
    *   **Change Signature:** `async def place_order(self, args: PlaceOrderArgs) -> Order:`
    *   **Logic:**
        *   Remove all existing individual parameter validation.
        *   Access parameters via `args.symbol`, `args.side`, `args.quantity`, etc.
        *   Update the call to `self._request_builder.build_place_order_payload` to use fields from `args`.

5.  **Update `BackpackAPI.place_order` (Implementation of Abstract Method):**
    *   **File:** `cyberdelta/apis/backpack/bp_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import PlaceOrderArgs`
    *   **Change Signature:** `async def place_order(self, args: PlaceOrderArgs) -> Order:`
    *   **Logic:** Change the call to `self.trading_service.place_order(args=args)`.
        *   The old multi-parameter call `self.trading_service.place_order(symbol=args.symbol, side=args.side, ...)` will be replaced by `self.trading_service.place_order(args=args)`.

6.  **Update `HyperliquidAPI.place_order` (Implementation of Abstract Method):**
    *   **File:** `cyberdelta/apis/hyperliquid/hl_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import PlaceOrderArgs`
    *   **Change Signature:** `async def place_order(self, args: PlaceOrderArgs) -> Order:`
    *   **Logic:** Change the call to `self.trading_service.place_order(args=args)`.
        *   The old multi-parameter call will be replaced.

7.  **Update Call Sites:**
    *   Search for all usages of `exchange_api_instance.place_order(...)` or `trading_service_instance.place_order(...)`.
    *   Modify these call sites to first instantiate `PlaceOrderArgs` and then pass it.
        ```python
        # Example (adjust import path as necessary):
        from cyberdelta.apis.models.service_args_models import PlaceOrderArgs
        from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce # etc.
        from decimal import Decimal

        # ...
        order_args = PlaceOrderArgs(
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.01"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("50000.00")
            # ... other fields as needed, optional ones can be omitted
        )
        # Check if `api` is ExchangeAPI or a specific TradingService to decide where to call
        # If `api` is ExchangeAPI:
        order_result = await exchange_api_instance.place_order(args=order_args)
        # If calling a trading service directly (less common from outside API layer):
        # order_result = await trading_service_instance.place_order(args=order_args)
        ```
    *   Pay close attention to tests, especially in `tests/unit/apis/services/` and `tests/integration/`.

8.  **Testing Requirements:**
    *   **New Tests for `PlaceOrderArgs`:** Create `tests/unit/apis/models/test_service_args_models.py`. Test `PlaceOrderArgs` validation thoroughly:
        *   Valid inputs for all fields.
        *   Invalid types for fields (e.g., non-string for symbol).
        *   Invalid values (e.g., negative quantity, zero price for limit order).
        *   Failures for inter-parameter dependencies (e.g., missing price for LIMIT, post-only with MARKET).
    *   **Update Existing Service Tests:** Modify tests for `BackpackTradingService.place_order` and `HyperliquidTradingService.place_order` to:
        *   Pass `PlaceOrderArgs` instances.
        *   Include test cases where `PlaceOrderArgs` validation itself should raise an error (e.g., `pytest.raises(ValidationError)` when trying to create `PlaceOrderArgs` with invalid data).
        *   Verify the services correctly unpack and use the arguments from the `PlaceOrderArgs` object.
    *   **Update API and Integration Tests:** Ensure any tests calling `place_order` at the `ExchangeAPI` level or higher are updated to the new signature.

9.  **Static Analysis and Reporting:**
    *   Run static analysis (Mypy, Pylint, Ruff) after changes and report any new warnings/errors.
    *   Ensure all changes strictly adhere to project rules.


---


**Prompt 1 for AI Coder (Angel): Refactor `transfer` Service Method Inputs with Pydantic Model**

**Project:** CyberDeltaEngine
**Context:**
Continuing our effort to enhance the robustness and clarity of our internal service layer interfaces, this task focuses on the `transfer` method. This method, used for internal fund transfers between account types within an exchange, can benefit from a Pydantic model to encapsulate its arguments, centralize validation, and improve its interface.

**Goal:**
1.  Define a `TransferArgs` Pydantic model in `cyberdelta/apis/models/service_args_models.py` for the `transfer` method's arguments.
2.  Refactor the abstract `ExchangeAPI.transfer` method to accept this `TransferArgs` model.
3.  Refactor the concrete implementations of `transfer` in `BackpackAPI`, `HyperliquidAPI`, and their respective account services (`BackpackAccountService`, `HyperliquidAccountService`) to use `TransferArgs`.
4.  Update all call sites of `transfer` to use the new `TransferArgs` model.

**Why:**
Encapsulating `transfer` arguments in a Pydantic model provides centralized validation, a clearer method signature, reduced boilerplate in service implementations, and improved testability and maintainability.

**What to do (Step-by-Step for `transfer`):**

1.  **Define `TransferArgs` Pydantic Model:**
    *   **File:** `cyberdelta/apis/models/service_args_models.py`
    *   **Imports needed:**
        ```python
        from decimal import Decimal
        from pydantic import BaseModel, Field, model_validator, field_validator, ValidationInfo, ConfigDict
        from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field
        ```
    *   **Model Definition (`TransferArgs`):**
        ```python
        class TransferArgs(BaseModel):
            model_config = ConfigDict(extra="forbid", validate_assignment=True)

            asset: str
            amount: Decimal = Field(gt=Decimal("0"))
            from_account_type: str # Specific validation might depend on exchange (e.g., Literal for known types)
            to_account_type: str   # Specific validation might depend on exchange
            client_transfer_id: str | None = Field(default=None)

            @field_validator("asset", "from_account_type", "to_account_type", mode="before")
            @classmethod
            def validate_required_strings(cls, v: Any, info: ValidationInfo) -> str:
                return validate_str_field(v, field_name=str(info.field_name), max_length=64, allow_empty=False)

            @field_validator("client_transfer_id", mode="before")
            @classmethod
            def validate_optional_string(cls, v: Any, info: ValidationInfo) -> str | None:
                if v is None:
                    return None
                return validate_str_field(v, field_name=str(info.field_name), max_length=128, allow_empty=False)

            @field_validator("amount", mode="before")
            @classmethod
            def parse_amount_decimal(cls, v: Any, info: ValidationInfo) -> Decimal:
                field_name = str(info.field_name)
                parsed = parse_decimal_value(v, field_name=field_name, allow_none=False)
                if parsed is None: # Should be caught by parse_decimal_value
                    raise ValueError(f"Field '{field_name}' is required and cannot be None or invalid.")
                if not parsed.is_finite():
                    raise ValueError(f"Field '{field_name}' must be a finite decimal, got {v}.")
                # Positivity (gt=0) is handled by Field constraint.
                return parsed

            @model_validator(mode="after")
            def check_account_types_differ(self) -> "TransferArgs":
                if self.from_account_type == self.to_account_type:
                    raise ValueError("from_account_type and to_account_type cannot be the same.")
                # Note: Exchange-specific validation for from/to_account_type values (e.g., against a Literal list
                # like {"SPOT", "MARGIN", "FUTURES"} for Backpack) would ideally be handled by derived
                # Args models (e.g., BackpackTransferArgs) or within the service if this generic model is used.
                # For now, this generic model only ensures they are non-empty strings and different.
                return self
        ```

2.  **Refactor `ExchangeAPI.transfer` (Abstract Method):**
    *   **File:** `cyberdelta/apis/base/exchange_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import TransferArgs`
    *   **Change Signature:**
        ```python
        @abstractmethod
        async def transfer(self, args: TransferArgs) -> Transfer: # Return type is core.models.operations.Transfer
            raise NotImplementedError
        ```

3.  **Refactor `BackpackAccountService.transfer`:**
    *   **File:** `cyberdelta/apis/backpack/services/bp_account_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import TransferArgs`
    *   **Change Signature:** `async def transfer(self, args: TransferArgs) -> Transfer:`
    *   **Logic:** Remove individual parameter validation. Access parameters from `args`. Update call to `self._request_builder.build_internal_transfer_payload`.

4.  **Refactor `HyperliquidAccountService.transfer`:**
    *   **File:** `cyberdelta/apis/hyperliquid/services/hl_account_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import TransferArgs`
    *   **Change Signature:** `async def transfer(self, args: TransferArgs) -> Transfer:`
    *   **Logic:** Remove individual parameter validation. Access parameters from `args`. Update call to `self._request_builder.build_l2_usd_transfer_payload` (note: Hyperliquid's transfer might have a different structure, so the builder method might differ, but it should take data from `args`).

5.  **Update `BackpackAPI.transfer`:**
    *   **File:** `cyberdelta/apis/backpack/bp_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import TransferArgs`
    *   **Change Signature:** `async def transfer(self, args: TransferArgs) -> Transfer:`
    *   **Logic:** Call `self.account_service.transfer(args=args)`.

6.  **Update `HyperliquidAPI.transfer`:**
    *   **File:** `cyberdelta/apis/hyperliquid/hl_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import TransferArgs`
    *   **Change Signature:** `async def transfer(self, args: TransferArgs) -> Transfer:`
    *   **Logic:** Call `self.account_service.transfer(args=args)`.

7.  **Update Call Sites:**
    *   Locate all calls to `transfer`.
    *   Modify them to instantiate and pass `TransferArgs`.

8.  **Testing Requirements:**
    *   Add unit tests for `TransferArgs` model validation in `tests/unit/apis/models/test_service_args_models.py`.
    *   Update service and API unit tests for `transfer` to use `TransferArgs`, including tests for validation failures.
    *   Ensure integration tests involving transfers are updated and pass.

9.  **Static Analysis and Reporting:**
    *   Run static analysis tools and report results.

---

**Prompt 2 for AI Coder (Angel): Refactor `withdraw` Service Method Inputs with Pydantic Model**

**Project:** CyberDeltaEngine
**Context:**
Following the pattern for `place_order` and `transfer`, we will refactor the `withdraw` method. This method handles fund withdrawals and often involves several parameters including asset, amount, address, network, and optional tags or IDs, making it suitable for argument encapsulation with a Pydantic model.

**Goal:**
1.  Define a `WithdrawArgs` Pydantic model in `cyberdelta/apis/models/service_args_models.py`.
2.  Refactor the abstract `ExchangeAPI.withdraw` method.
3.  Refactor concrete implementations in `BackpackAPI`, `HyperliquidAPI`, and their account services.
4.  Update all call sites.

**Why:**
Consistent use of Pydantic models for complex service method arguments improves API design, centralizes validation, reduces code duplication, and enhances testability and maintainability.

**What to do (Step-by-Step for `withdraw`):**

1.  **Define `WithdrawArgs` Pydantic Model:**
    *   **File:** `cyberdelta/apis/models/service_args_models.py`
    *   **Imports (similar to `TransferArgs`):**
        ```python
        from decimal import Decimal
        from typing import Any # For kwargs
        from pydantic import BaseModel, Field, field_validator, ValidationInfo, ConfigDict
        from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field
        ```
    *   **Model Definition (`WithdrawArgs`):**
        ```python
        class WithdrawArgs(BaseModel):
            model_config = ConfigDict(extra="allow", validate_assignment=True) # extra="allow" for **kwargs

            asset: str
            amount: Decimal = Field(gt=Decimal("0"))
            address: str
            network: str | None = Field(default=None) # Network can be optional for some exchanges or implicit
            tag: str | None = Field(default=None) # e.g., memo for XRP, destination tag for others
            client_withdrawal_id: str | None = Field(default=None)
            two_factor_token: str | None = Field(default=None) # If 2FA is handled at this level
            # kwargs: dict[str, Any] = Field(default_factory=dict) # To capture extra exchange-specific params

            @field_validator("asset", "address", mode="before")
            @classmethod
            def validate_required_strings(cls, v: Any, info: ValidationInfo) -> str:
                return validate_str_field(v, field_name=str(info.field_name), max_length=128, allow_empty=False) # Address can be long

            @field_validator("network", "tag", "client_withdrawal_id", "two_factor_token", mode="before")
            @classmethod
            def validate_optional_strings(cls, v: Any, info: ValidationInfo) -> str | None:
                if v is None:
                    return None
                # Shorter max_length for network/tag unless specific exchanges require longer
                return validate_str_field(v, field_name=str(info.field_name), max_length=64, allow_empty=False)

            @field_validator("amount", mode="before")
            @classmethod
            def parse_amount_decimal(cls, v: Any, info: ValidationInfo) -> Decimal:
                field_name = str(info.field_name)
                parsed = parse_decimal_value(v, field_name=field_name, allow_none=False)
                if parsed is None: # Should be caught by parse_decimal_value
                    raise ValueError(f"Field '{field_name}' is required and cannot be None or invalid.")
                if not parsed.is_finite():
                    raise ValueError(f"Field '{field_name}' must be a finite decimal, got {v}.")
                # Positivity (gt=0) is handled by Field constraint.
                return parsed

            # No model_validator needed for basic WithdrawArgs unless inter-dependencies are identified
            # that are universal. Exchange-specific checks (e.g., tag required for certain asset/network)
            # would go into the service method or a derived Args model.
        ```

2.  **Refactor `ExchangeAPI.withdraw` (Abstract Method):**
    *   **File:** `cyberdelta/apis/base/exchange_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import WithdrawArgs`
    *   **Change Signature:**
        ```python
        @abstractmethod
        async def withdraw(self, args: WithdrawArgs, **kwargs: Any) -> Withdrawal: # Return core.models.operations.Withdrawal
            # Pass through **kwargs from API client to service
            raise NotImplementedError
        ```
        *Note: The `**kwargs` in the abstract method signature is important if `BackpackAPI` or `HyperliquidAPI` implementations pass extra arguments directly. The `WithdrawArgs` model also has `extra="allow"` to capture these into its own `kwargs` field if they are not explicitly defined.*

3.  **Refactor `BackpackAccountService.withdraw`:**
    *   **File:** `cyberdelta/apis/backpack/services/bp_account_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import WithdrawArgs`
    *   **Change Signature:** `async def withdraw(self, args: WithdrawArgs, **service_kwargs: Any) -> Withdrawal:`
    *   **Logic:** Remove individual parameter validation. Access parameters from `args`. Update call to `self._request_builder.build_withdraw_payload`. Pass `args.model_extra` (if Pydantic v2, or `args.kwargs` if Pydantic v1 with custom field) or `service_kwargs` to the builder if it handles arbitrary `**kwargs`.

4.  **Refactor `HyperliquidAccountService.withdraw`:**
    *   **File:** `cyberdelta/apis/hyperliquid/services/hl_account_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import WithdrawArgs`
    *   **Change Signature:** `async def withdraw(self, args: WithdrawArgs, **service_kwargs: Any) -> Withdrawal:`
    *   **Logic:** Remove individual parameter validation. Access parameters from `args`. Update call to `self._request_builder.build_withdrawal_payload`.

5.  **Update `BackpackAPI.withdraw`:**
    *   **File:** `cyberdelta/apis/backpack/bp_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import WithdrawArgs`
    *   **Change Signature:** `async def withdraw(self, args: WithdrawArgs, **kwargs: Any) -> Withdrawal:`
    *   **Logic:** Call `self.account_service.withdraw(args=args, **kwargs)`.

6.  **Update `HyperliquidAPI.withdraw`:**
    *   **File:** `cyberdelta/apis/hyperliquid/hl_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import WithdrawArgs`
    *   **Change Signature:** `async def withdraw(self, args: WithdrawArgs, **kwargs: Any) -> Withdrawal:`
    *   **Logic:** Call `self.account_service.withdraw(args=args, **kwargs)`.

7.  **Update Call Sites:**
    *   Locate all calls to `withdraw`.
    *   Modify them to instantiate and pass `WithdrawArgs`. Any previous `**kwargs` passed to `withdraw` should now either be part of `WithdrawArgs` if common, or will be captured by `WithdrawArgs.model_config(extra="allow")` and can be accessed via `args.model_extra` (Pydantic v2) if needed by the service.

8.  **Testing Requirements:**
    *   Add unit tests for `WithdrawArgs` model validation in `tests/unit/apis/models/test_service_args_models.py`.
    *   Update service and API unit tests for `withdraw` to use `WithdrawArgs`, including tests for validation failures.
    *   Ensure integration tests involving withdrawals are updated and pass.

9.  **Static Analysis and Reporting:**
    *   Run static analysis tools and report results.

