
**Prompt 1 for AI Coder (Angel): Refactor `get_order_history` Service Method Inputs with Pydantic Model**

**Project:** CyberDeltaEngine
**Context:**
We are continuing to improve our service layer interfaces by using Pydantic models for complex method arguments. The `get_order_history` method, which often involves multiple optional filtering parameters like symbol, time range, limits, and various order identifiers, is a good candidate for this refactoring.

**Goal:**
1.  Define a `GetOrderHistoryArgs` Pydantic model in `cyberdelta/apis/models/service_args_models.py` to encapsulate all arguments for fetching order history.
2.  Refactor the abstract `ExchangeAPI.get_order_history` method to accept this `GetOrderHistoryArgs` model.
3.  Refactor the concrete implementations of `get_order_history` in `BackpackAPI`, `HyperliquidAPI`, and their respective account services (`BackpackAccountService`, `HyperliquidAccountService`) to use `GetOrderHistoryArgs`.
4.  Update all call sites of `get_order_history` to use the new `GetOrderHistoryArgs` model.

**Why:**
Using a Pydantic model for `get_order_history` arguments will centralize validation (e.g., start_time before end_time, positive limit), make the method signature cleaner, reduce boilerplate in service implementations, and improve testability and maintainability.

**What to do (Step-by-Step for `get_order_history`):**

1.  **Define `GetOrderHistoryArgs` Pydantic Model:**
    *   **File:** `cyberdelta/apis/models/service_args_models.py`
    *   **Imports needed:**
        ```python
        from datetime import datetime
        from pydantic import BaseModel, Field, model_validator, field_validator, ValidationInfo, ConfigDict
        from cyberdelta.utils.parsing import parse_datetime_utc, validate_str_field
        ```
    *   **Model Definition (`GetOrderHistoryArgs`):**
        ```python
        class GetOrderHistoryArgs(BaseModel):
            model_config = ConfigDict(extra="forbid", validate_assignment=True)

            symbol: str | None = Field(default=None)
            start_time: datetime | None = Field(default=None)
            end_time: datetime | None = Field(default=None)
            limit: int | None = Field(default=None, gt=0) # Limit must be positive if provided
            order_id: str | None = Field(default=None)
            client_order_id: str | None = Field(default=None)
            # Add any other common filtering parameters here if they exist across exchanges

            @field_validator("symbol", "order_id", "client_order_id", mode="before")
            @classmethod
            def validate_optional_strings(cls, v: Any, info: ValidationInfo) -> str | None:
                if v is None:
                    return None
                # Assuming generic string validation for these, max_length can be adjusted
                return validate_str_field(v, field_name=str(info.field_name), max_length=64, allow_empty=False)

            @field_validator("start_time", "end_time", mode="before")
            @classmethod
            def parse_optional_datetime_utc(cls, v: Any, info: ValidationInfo) -> datetime | None:
                if v is None:
                    return None
                # parse_datetime_utc will return None if parsing fails, which is acceptable for optional fields
                return parse_datetime_utc(v, field_name=str(info.field_name))

            @field_validator("limit", mode="before")
            @classmethod
            def parse_optional_int(cls, v: Any, info: ValidationInfo) -> int | None:
                if v is None:
                    return None
                if not isinstance(v, int | str | float): # Allow int, or str/float that can be int
                    raise ValueError(f"Field '{str(info.field_name)}' must be an integer or convertible to one.")
                try:
                    int_val = int(v)
                    # Positivity (gt=0) is handled by Field constraint
                    return int_val
                except ValueError as e:
                    raise ValueError(f"Field '{str(info.field_name)}' could not be converted to int: {v}") from e

            @model_validator(mode="after")
            def check_time_range(self) -> "GetOrderHistoryArgs":
                if self.start_time and self.end_time and self.start_time >= self.end_time:
                    raise ValueError("start_time must be before end_time.")
                # Backpack specific: Only one of orderId or clientId can be used.
                # This validation might be too specific for a generic Args model.
                # Could be enforced in BackpackAccountService or a BackpackGetOrderHistoryArgs derived model.
                # if self.order_id and self.client_order_id:
                #     raise ValueError("Provide either order_id or client_order_id, not both (for Backpack).")
                return self
        ```

2.  **Refactor `ExchangeAPI.get_order_history` (Abstract Method):**
    *   **File:** `cyberdelta/apis/base/exchange_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetOrderHistoryArgs`
    *   **Change Signature:**
        ```python
        @abstractmethod
        async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:
            raise NotImplementedError
        ```

3.  **Refactor `BackpackAccountService.get_order_history`:**
    *   **File:** `cyberdelta/apis/backpack/services/bp_account_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetOrderHistoryArgs`
    *   **Change Signature:** `async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:`
    *   **Logic:** Remove individual parameter validation. Access parameters from `args`. Update call to `self._request_builder.build_get_order_history_params`.
        *   **Backpack Specific:** Backpack's builder for order history params might need `start_time_ms` and `end_time_ms`. Convert `args.start_time` and `args.end_time` (datetime objects) to milliseconds if they are not None.

4.  **Refactor `HyperliquidAccountService.get_order_history`:**
    *   **File:** `cyberdelta/apis/hyperliquid/services/hl_account_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetOrderHistoryArgs`
    *   **Change Signature:** `async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:`
    *   **Logic:** Remove individual parameter validation. Access parameters from `args`. Update call to `self._request_builder.build_order_history_payload`.
        *   **Hyperliquid Specific:** This builder requires `start_time_ms` and `end_time_ms`. Convert `args.start_time` and `args.end_time` (datetime objects) to milliseconds. Hyperliquid does not use `limit`, `order_id`, or `client_order_id` in its `queryOrderHistory` request payload in the same way, so these fields from `args` might be ignored or used for client-side filtering if applicable.

5.  **Update `BackpackAPI.get_order_history`:**
    *   **File:** `cyberdelta/apis/backpack/bp_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetOrderHistoryArgs`
    *   **Change Signature:** `async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:`
    *   **Logic:** Call `self.account_service.get_order_history(args=args)`.

6.  **Update `HyperliquidAPI.get_order_history`:**
    *   **File:** `cyberdelta/apis/hyperliquid/hl_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetOrderHistoryArgs`
    *   **Change Signature:** `async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:`
    *   **Logic:** Call `self.account_service.get_order_history(args=args)`.

7.  **Update Call Sites:**
    *   Locate all calls to `get_order_history`.
    *   Modify them to instantiate and pass `GetOrderHistoryArgs`.

8.  **Testing Requirements:**
    *   Add unit tests for `GetOrderHistoryArgs` model validation.
    *   Update service and API unit tests for `get_order_history` to use `GetOrderHistoryArgs`.
    *   Ensure integration tests involving order history fetching are updated and pass.

9.  **Static Analysis and Reporting:**
    *   Run static analysis tools and report results.

---

**Prompt 2 for AI Coder (Angel): Refactor `get_market_data` Service Method Inputs with Pydantic Model**

**Project:** CyberDeltaEngine
**Context:**
We are continuing to standardize service method inputs using Pydantic models. The `get_market_data` method, used to fetch historical klines/candlesticks, often has parameters for symbol, timeframe, limit, and optional time ranges, making it suitable for this refactoring approach.

**Goal:**
1.  Define a `GetMarketDataArgs` Pydantic model in `cyberdelta/apis/models/service_args_models.py`.
2.  Refactor the abstract `ExchangeAPI.get_market_data` method.
3.  Refactor concrete implementations in `BackpackAPI`, `HyperliquidAPI`, and their market data services.
4.  Update all call sites.

**Why:**
This refactoring will provide centralized validation for market data request parameters (e.g., valid timeframe format, positive limit, logical time range), cleaner service method signatures, and better maintainability.

**What to do (Step-by-Step for `get_market_data`):**

1.  **Define `GetMarketDataArgs` Pydantic Model:**
    *   **File:** `cyberdelta/apis/models/service_args_models.py`
    *   **Imports:**
        ```python
        from typing import Any # For Any in validators if needed
        from pydantic import BaseModel, Field, model_validator, field_validator, ValidationInfo, ConfigDict
        from cyberdelta.utils.parsing import validate_str_field # Add other parsing utils if used
        ```
    *   **Model Definition (`GetMarketDataArgs`):**
        ```python
        class GetMarketDataArgs(BaseModel):
            model_config = ConfigDict(extra="forbid", validate_assignment=True)

            symbol: str
            timeframe: str # Could be Literal if universal timeframes are defined
            limit: int | None = Field(default=100, gt=0) # Default matches BackpackAPI
            start_time_ms: int | None = Field(default=None, gt=0) # Timestamp in milliseconds
            end_time_ms: int | None = Field(default=None, gt=0)   # Timestamp in milliseconds

            @field_validator("symbol", "timeframe", mode="before")
            @classmethod
            def validate_required_strings(cls, v: Any, info: ValidationInfo) -> str:
                # Timeframe might have specific format constraints per exchange (e.g., "1m", "1H")
                # For a generic model, basic string validation. Specifics can be in service layer.
                max_len = 64 if str(info.field_name) == "symbol" else 16 # Shorter for timeframe
                return validate_str_field(v, field_name=str(info.field_name), max_length=max_len, allow_empty=False)

            @field_validator("limit", "start_time_ms", "end_time_ms", mode="before")
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

            @model_validator(mode="after")
            def check_time_range_logic(self) -> "GetMarketDataArgs":
                if self.start_time_ms and self.end_time_ms and self.start_time_ms >= self.end_time_ms:
                    raise ValueError("start_time_ms must be before end_time_ms if both are provided.")
                # Additional checks like limit constraints (e.g., max 1000) could be added if universal,
                # otherwise, they belong in exchange-specific RequestBuilder or service logic.
                return self
        ```

2.  **Refactor `ExchangeAPI.get_market_data` (Abstract Method):**
    *   **File:** `cyberdelta/apis/base/exchange_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetMarketDataArgs`, `from cyberdelta.core.models.market.candle import Candle`
    *   **Change Signature:**
        ```python
        @abstractmethod
        async def get_market_data(self, args: GetMarketDataArgs) -> list[Candle]:
            raise NotImplementedError
        ```

3.  **Refactor `BackpackMarketDataService.get_market_data`:**
    *   **File:** `cyberdelta/apis/backpack/services/bp_market_data_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetMarketDataArgs`
    *   **Change Signature:** `async def get_market_data(self, args: GetMarketDataArgs) -> list[Candle]:`
    *   **Logic:** Remove individual parameter validation. Access parameters from `args`. Update call to `self._request_builder.build_get_market_data_params`.
        *   **Backpack Specific:** The builder expects `timeframe_str` as a Literal. The service method needs to ensure `args.timeframe` conforms to this or handle mapping/validation if `args.timeframe` is more generic. For now, assume `args.timeframe` string is directly usable.

4.  **Refactor `HyperliquidMarketDataService.get_market_data`:**
    *   **File:** `cyberdelta/apis/hyperliquid/services/hl_market_data_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetMarketDataArgs`
    *   **Change Signature:** `async def get_market_data(self, args: GetMarketDataArgs) -> list[Candle]:`
    *   **Logic:** Remove individual parameter validation. Access parameters from `args`. Update call to `self._request_builder.build_candle_snapshot_payload` (which takes `symbol`, `timeframe` (as interval), `start_time_ms`, `end_time_ms`).

5.  **Update `BackpackAPI.get_market_data`:**
    *   **File:** `cyberdelta/apis/backpack/bp_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetMarketDataArgs`
    *   **Change Signature:** `async def get_market_data(self, args: GetMarketDataArgs) -> list[Candle]:`
    *   **Logic:** Call `self.market_data_service.get_market_data(args=args)`.

6.  **Update `HyperliquidAPI.get_market_data`:**
    *   **File:** `cyberdelta/apis/hyperliquid/hl_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetMarketDataArgs`
    *   **Change Signature:** `async def get_market_data(self, args: GetMarketDataArgs) -> list[Candle]:`
    *   **Logic:** Call `self.market_data_service.get_market_data(args=args)`.

7.  **Update Call Sites:**
    *   Locate all calls to `get_market_data`.
    *   Modify them to instantiate and pass `GetMarketDataArgs`.

8.  **Testing Requirements:**
    *   Add unit tests for `GetMarketDataArgs` model validation.
    *   Update service and API unit tests for `get_market_data` to use `GetMarketDataArgs`.
    *   Ensure integration tests involving market data fetching are updated and pass.

9.  **Static Analysis and Reporting:**
    *   Run static analysis tools and report results.

