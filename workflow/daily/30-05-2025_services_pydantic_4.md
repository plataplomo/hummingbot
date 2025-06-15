
**Prompt 1 for AI Coder (Angel): Refactor `get_trade_history` Service Method Inputs with Pydantic Model**

**Project:** CyberDeltaEngine
**Context:**
Continuing our Pydantic modeling efforts for service layer inputs, we will now refactor the `get_trade_history` method. This method fetches historical trades (fills) for an account and typically accepts parameters like `symbol` and `limit`.

**Goal:**
1.  Define a `GetTradeHistoryArgs` Pydantic model in `cyberdelta/apis/models/service_args_models.py`.
2.  Refactor the abstract `ExchangeAPI.get_trade_history` method.
3.  Refactor concrete implementations in `BackpackAPI`, `HyperliquidAPI`, and their respective account services.
4.  Update all call sites.

**Why:**
Using a Pydantic model for `get_trade_history` arguments will ensure consistent validation of parameters like `symbol` (if provided) and `limit` (e.g., ensuring it's a positive integer).

**What to do (Step-by-Step for `get_trade_history`):**

1.  **Define `GetTradeHistoryArgs` Pydantic Model:**
    *   **File:** `cyberdelta/apis/models/service_args_models.py`
    *   **Imports:**
        ```python
        from typing import Any # For Any in validators
        from pydantic import BaseModel, Field, field_validator, ValidationInfo, ConfigDict
        from cyberdelta.utils.parsing import validate_str_field
        ```
    *   **Model Definition (`GetTradeHistoryArgs`):**
        ```python
        class GetTradeHistoryArgs(BaseModel):
            model_config = ConfigDict(extra="forbid", validate_assignment=True)

            symbol: str | None = Field(default=None)
            limit: int | None = Field(default=100, gt=0) # Default matches BackpackAPI
            # Add other common parameters like start_time_ms, end_time_ms, from_id if applicable universally
            # For example, Backpack's builder supports these:
            # start_time_ms: int | None = Field(default=None, gt=0)
            # end_time_ms: int | None = Field(default=None, gt=0)
            # from_id: str | None = Field(default=None)

            @field_validator("symbol", mode="before") # Add 'from_id' if included above
            @classmethod
            def validate_optional_strings(cls, v: Any, info: ValidationInfo) -> str | None:
                if v is None:
                    return None
                return validate_str_field(v, field_name=str(info.field_name), max_length=64, allow_empty=False)

            @field_validator("limit", mode="before") # Add 'start_time_ms', 'end_time_ms' if included
            @classmethod
            def parse_optional_positive_int(cls, v: Any, info: ValidationInfo) -> int | None:
                if v is None:
                    return None
                if not isinstance(v, int | str | float):
                    raise ValueError(f"Field '{str(info.field_name)}' must be an integer or convertible.")
                try:
                    int_val = int(v)
                    # Positivity (gt=0) is handled by Field constraint.
                    return int_val
                except ValueError as e:
                    raise ValueError(f"Field '{str(info.field_name)}' could not be converted to int: {v}") from e

            # @model_validator(mode="after") # If start/end times are added
            # def check_time_range_logic(self) -> "GetTradeHistoryArgs":
            #     if self.start_time_ms and self.end_time_ms and self.start_time_ms >= self.end_time_ms:
            #         raise ValueError("start_time_ms must be before end_time_ms if both are provided.")
            #     return self
        ```

2.  **Refactor `ExchangeAPI.get_trade_history` (Abstract Method):**
    *   **File:** `cyberdelta/apis/base/exchange_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetTradeHistoryArgs`, `from cyberdelta.core.models import Trade`
    *   **Change Signature:**
        ```python
        @abstractmethod
        async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Trade]:
            raise NotImplementedError
        ```

3.  **Refactor `BackpackAccountService.get_trade_history`:**
    *   **File:** `cyberdelta/apis/backpack/services/bp_account_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetTradeHistoryArgs`
    *   **Change Signature:** `async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Trade]:`
    *   **Logic:** Remove parameter validation. Access parameters from `args`. Update call to `self._request_builder.build_get_trade_history_params`.

4.  **Refactor `HyperliquidAccountService.get_trade_history`:**
    *   **File:** `cyberdelta/apis/hyperliquid/services/hl_account_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetTradeHistoryArgs`
    *   **Change Signature:** `async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Trade]:`
    *   **Logic:** Remove parameter validation. Access parameters from `args`. Update call to `self._request_builder.build_user_fills_request_payload` (Hyperliquid uses `/info` with type `userFills` and only `user` (wallet address) as param; `symbol` and `limit` from `args` would be for client-side filtering after fetching all user fills).

5.  **Update `BackpackAPI.get_trade_history`:**
    *   **File:** `cyberdelta/apis/backpack/bp_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetTradeHistoryArgs`
    *   **Change Signature:** `async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Trade]:`
    *   **Logic:** Call `self.account_service.get_trade_history(args=args)`.

6.  **Update `HyperliquidAPI.get_trade_history`:**
    *   **File:** `cyberdelta/apis/hyperliquid/hl_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetTradeHistoryArgs`
    *   **Change Signature:** `async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Trade]:`
    *   **Logic:** Call `self.account_service.get_trade_history(args=args)`.

7.  **Update Call Sites:**
    *   Locate calls to `get_trade_history`.
    *   Modify them to instantiate and pass `GetTradeHistoryArgs`.

8.  **Testing Requirements:**
    *   Add unit tests for `GetTradeHistoryArgs`.
    *   Update service and API unit tests for `get_trade_history`.
    *   Ensure integration tests pass.

9.  **Static Analysis and Reporting:**
    *   Run static analysis tools and report results.

---
**Prompt 2 for AI Coder (Angel): Refactor `get_all_open_orders` Service Method Inputs with Pydantic Model**

**Project:** CyberDeltaEngine
**Context:**
The `get_all_open_orders` method is an alias or specific variant for fetching open orders, often accepting an optional `symbol` filter. To maintain consistency and ensure validated inputs, we'll refactor it to use a Pydantic argument model.

**Goal:**
1.  Define a `GetAllOpenOrdersArgs` Pydantic model (it might be very similar or identical to a potential `GetOpenOrdersArgs` if that also primarily takes a symbol).
2.  Refactor the abstract `ExchangeAPI.get_all_open_orders` method.
3.  Refactor concrete implementations in `BackpackAPI`, `HyperliquidAPI`, and their relevant services.
4.  Update all call sites.

**Why:**
Standardizes input handling for fetching all open orders, ensuring the optional symbol parameter is correctly validated if provided.

**What to do (Step-by-Step for `get_all_open_orders`):**

1.  **Define `GetAllOpenOrdersArgs` Pydantic Model:**
    *   **File:** `cyberdelta/apis/models/service_args_models.py`
    *   **Imports:**
        ```python
        from typing import Any # For Any in validators
        from pydantic import BaseModel, Field, field_validator, ValidationInfo, ConfigDict
        from cyberdelta.utils.parsing import validate_str_field
        ```
    *   **Model Definition (`GetAllOpenOrdersArgs`):**
        ```python
        class GetAllOpenOrdersArgs(BaseModel):
            model_config = ConfigDict(extra="forbid", validate_assignment=True)

            symbol: str | None = Field(default=None) # Optional symbol to filter by

            @field_validator("symbol", mode="before")
            @classmethod
            def validate_optional_symbol(cls, v: Any, info: ValidationInfo) -> str | None:
                if v is None:
                    return None
                return validate_str_field(v, field_name=str(info.field_name), max_length=64, allow_empty=False)
        ```
        *Note: If `GetOpenOrdersArgs` is already defined and is identical, you can reuse it or create `GetAllOpenOrdersArgs = GetOpenOrdersArgs`.*

2.  **Refactor `ExchangeAPI.get_all_open_orders` (Abstract Method):**
    *   **File:** `cyberdelta/apis/base/exchange_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetAllOpenOrdersArgs`, `from cyberdelta.core.models import Order`
    *   **Change Signature:**
        ```python
        @abstractmethod
        async def get_all_open_orders(self, args: GetAllOpenOrdersArgs) -> list[Order]:
            raise NotImplementedError
        ```

3.  **Refactor `BackpackTradingService.get_all_open_orders`:**
    *   **File:** `cyberdelta/apis/backpack/services/bp_trading_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetAllOpenOrdersArgs`
    *   **Change Signature:** `async def get_all_open_orders(self, args: GetAllOpenOrdersArgs) -> list[Order]:`
    *   **Logic:** This method likely delegates to `self.get_open_orders`. Update the call to `self.get_open_orders` to potentially construct its own `GetOpenOrdersArgs` from `args.symbol` or pass `args` if the signatures align. If `GetOpenOrdersArgs` and `GetAllOpenOrdersArgs` are identical, direct passing is fine.

4.  **Refactor `HyperliquidTradingService.get_all_open_orders` (or `HyperliquidAccountService` if `get_open_orders` is there):**
    *   **File:** (e.g., `cyberdelta/apis/hyperliquid/services/hl_trading_service.py` or `hl_account_service.py`)
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetAllOpenOrdersArgs`
    *   **Change Signature:** `async def get_all_open_orders(self, args: GetAllOpenOrdersArgs) -> list[Order]:`
    *   **Logic:** Similar to Backpack, if this is an alias, update how it calls the main `get_open_orders` method, passing `args.symbol`. Hyperliquid's open orders are fetched by `user_address` and then client-side filtered by symbol if `args.symbol` is provided.

5.  **Update `BackpackAPI.get_all_open_orders`:**
    *   **File:** `cyberdelta/apis/backpack/bp_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetAllOpenOrdersArgs`
    *   **Change Signature:** `async def get_all_open_orders(self, args: GetAllOpenOrdersArgs) -> list[Order]:`
    *   **Logic:** Call `self.trading_service.get_all_open_orders(args=args)`.

6.  **Update `HyperliquidAPI.get_all_open_orders`:**
    *   **File:** `cyberdelta/apis/hyperliquid/hl_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetAllOpenOrdersArgs`
    *   **Change Signature:** `async def get_all_open_orders(self, args: GetAllOpenOrdersArgs) -> list[Order]:`
    *   **Logic:** Call `self.trading_service.get_all_open_orders(args=args)` (or `self.account_service` if appropriate for HL).

7.  **Update Call Sites:**
    *   Locate calls to `get_all_open_orders`.
    *   Modify them to instantiate and pass `GetAllOpenOrdersArgs`. E.g., `GetAllOpenOrdersArgs(symbol="BTC-PERP")` or `GetAllOpenOrdersArgs()` for all.

8.  **Testing Requirements:**
    *   Add unit tests for `GetAllOpenOrdersArgs`.
    *   Update service and API unit tests for `get_all_open_orders`.
    *   Ensure integration tests pass.

9.  **Static Analysis and Reporting:**
    *   Run static analysis tools and report results.

