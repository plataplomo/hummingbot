
**Prompt 1 for AI Coder (Angel): Refactor `get_order` Service Method Inputs with Pydantic Model**

**Project:** CyberDeltaEngine
**Context:**
We are continuing to standardize service method inputs using Pydantic models. The `get_order` method, used to fetch details of a specific order, typically requires an `order_id` and may accept an optional `symbol` and `client_order_id` for more precise lookup or exchange-specific requirements.

**Goal:**
1.  Define a `GetOrderArgs` Pydantic model in `cyberdelta/apis/models/service_args_models.py`.
2.  Refactor the abstract `ExchangeAPI.get_order` method to accept this `GetOrderArgs` model.
3.  Refactor concrete implementations in `BackpackAPI`, `HyperliquidAPI`, and their relevant trading services.
4.  Update all call sites.

**Why:**
Using a Pydantic model ensures that `order_id` is a valid string, and optional parameters like `symbol` and `client_order_id` are handled consistently with validation. It clarifies the method's input contract.

**What to do (Step-by-Step for `get_order`):**

1.  **Define `GetOrderArgs` Pydantic Model:**
    *   **File:** `cyberdelta/apis/models/service_args_models.py`
    *   **Imports:**
        ```python
        from typing import Any # For Any in validators
        from pydantic import BaseModel, Field, model_validator, field_validator, ValidationInfo, ConfigDict
        from cyberdelta.utils.parsing import validate_str_field
        ```
    *   **Model Definition (`GetOrderArgs`):**
        ```python
        class GetOrderArgs(BaseModel):
            model_config = ConfigDict(extra="forbid", validate_assignment=True)

            order_id: str # Primary identifier, usually exchange-generated
            symbol: str | None = Field(default=None) # Often required or recommended by exchanges
            client_order_id: str | None = Field(default=None) # Alternative identifier

            @field_validator("order_id", "symbol", "client_order_id", mode="before")
            @classmethod
            def validate_strings(cls, v: Any, info: ValidationInfo) -> str | None:
                field_name = str(info.field_name)
                is_required = field_name == "order_id"

                if v is None:
                    if is_required:
                        raise ValueError(f"Field '{field_name}' is required.")
                    return None # For optional fields

                # Max length for order_id can be quite long for some exchanges (e.g. UUIDs)
                max_len = 128 if field_name == "order_id" or field_name == "client_order_id" else 64
                return validate_str_field(v, field_name=field_name, max_length=max_len, allow_empty=False)

            @model_validator(mode="after")
            def check_identifier_logic(self) -> "GetOrderArgs":
                # While order_id is primary, some exchanges might heavily rely on symbol.
                # Backpack requires symbol for its GET /order/{id} endpoint as a query param.
                # Hyperliquid's orderStatus needs user + oid, symbol is not directly part of request.
                # For a generic model, ensuring order_id is primary.
                # A common pattern is that at least one identifier (order_id or client_order_id)
                # must be present. Since order_id is mandatory here, that's covered.
                # If symbol becomes strictly required for all exchanges, it can be made non-optional.
                if self.symbol is None:
                     # Log a debug message or warning if symbol is often needed but not provided.
                     # logger.debug(f"GetOrderArgs created without a symbol for order_id {self.order_id}")
                     pass
                return self
        ```

2.  **Refactor `ExchangeAPI.get_order` (Abstract Method):**
    *   **File:** `cyberdelta/apis/base/exchange_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetOrderArgs`, `from cyberdelta.core.models import Order`
    *   **Change Signature:**
        ```python
        @abstractmethod
        async def get_order(self, args: GetOrderArgs) -> Order | None:
            raise NotImplementedError
        ```

3.  **Refactor `BackpackTradingService.get_order`:**
    *   **File:** `cyberdelta/apis/backpack/services/bp_trading_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetOrderArgs`
    *   **Change Signature:** `async def get_order(self, args: GetOrderArgs) -> Order | None:`
    *   **Logic:** Remove individual parameter validation. Access parameters from `args`. Update call to `self._request_builder.build_get_order_params` (which takes `symbol`) and ensure the correct identifier (`args.order_id` or `args.client_order_id`) is used in the endpoint path. Backpack's `get_order` service method requires `symbol`. The Pydantic model makes `symbol` optional, so the service method must enforce its presence if Backpack's API requires it (which it does for the path `GET /order/{orderIdOrClientId}?symbol=...`).

4.  **Refactor `HyperliquidTradingService.get_order`:**
    *   **File:** `cyberdelta/apis/hyperliquid/services/hl_trading_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetOrderArgs`
    *   **Change Signature:** `async def get_order(self, args: GetOrderArgs) -> Order | None:`
    *   **Logic:** Remove individual parameter validation. Access parameters from `args`. Convert `args.order_id` to `int`. Call `self._get_order_status_raw` which uses `user` (wallet address) and `oid`. The `symbol` from `args` might be used for context or mapping if needed post-fetch.

5.  **Update `BackpackAPI.get_order`:**
    *   **File:** `cyberdelta/apis/backpack/bp_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetOrderArgs`
    *   **Change Signature:** `async def get_order(self, args: GetOrderArgs) -> Order | None:`
    *   **Logic:** Call `self.trading_service.get_order(args=args)`.

6.  **Update `HyperliquidAPI.get_order`:**
    *   **File:** `cyberdelta/apis/hyperliquid/hl_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetOrderArgs`
    *   **Change Signature:** `async def get_order(self, args: GetOrderArgs) -> Order | None:`
    *   **Logic:** Call `self.trading_service.get_order(args=args)`.

7.  **Update Call Sites:**
    *   Locate all calls to `get_order`.
    *   Modify them to instantiate and pass `GetOrderArgs`.

8.  **Testing Requirements:**
    *   Add unit tests for `GetOrderArgs` model validation.
    *   Update service and API unit tests for `get_order`.
    *   Ensure integration tests pass.

9.  **Static Analysis and Reporting:**
    *   Run static analysis tools and report results.

---
**Prompt 2 for AI Coder (Angel): Refactor `get_order_status` Service Method Inputs with Pydantic Model**

**Project:** CyberDeltaEngine
**Context:**
The `get_order_status` method is very similar to `get_order`, often acting as an alias or sharing significant implementation logic. We will refactor its inputs using a Pydantic model, `GetOrderStatusArgs`, for consistency and robustness.

**Goal:**
1.  Define a `GetOrderStatusArgs` Pydantic model in `cyberdelta/apis/models/service_args_models.py`. This model will likely be identical to `GetOrderArgs`. If so, Angel can alias it or ensure they are kept in sync.
2.  Refactor the abstract `ExchangeAPI.get_order_status` method.
3.  Refactor concrete implementations in `BackpackAPI`, `HyperliquidAPI`, and their relevant services.
4.  Update all call sites.

**Why:**
Standardizes input handling for fetching order status, ensuring identifiers and optional parameters are validated consistently.

**What to do (Step-by-Step for `get_order_status`):**

1.  **Define/Alias `GetOrderStatusArgs` Pydantic Model:**
    *   **File:** `cyberdelta/apis/models/service_args_models.py`
    *   **Action:**
        *   If `GetOrderArgs` (defined in the previous prompt) perfectly fits the needs of `get_order_status` (i.e., same parameters: `order_id`, optional `symbol`, optional `client_order_id`), then simply use `GetOrderArgs` for this method. No new model definition is strictly needed.
        *   If there are subtle differences or a desire for semantic distinction, define `GetOrderStatusArgs` identically to `GetOrderArgs` or derive from it if appropriate. For simplicity, let's assume they are identical for now and can reuse `GetOrderArgs`. If Angel finds a need for distinction, they can create `GetOrderStatusArgs`.
    *   **If defining new (or for clarity, copy `GetOrderArgs`):**
        ```python
        # In cyberdelta/apis/models/service_args_models.py
        # class GetOrderStatusArgs(GetOrderArgs): # Or copy fields if no inheritance desired
        #     pass
        # For now, assume GetOrderArgs is sufficient.
        from .service_args_models import GetOrderArgs # Assuming GetOrderArgs is already there
        GetOrderStatusArgs = GetOrderArgs # Alias for clarity in signatures
        ```

2.  **Refactor `ExchangeAPI.get_order_status` (Abstract Method):**
    *   **File:** `cyberdelta/apis/base/exchange_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetOrderArgs` (or `GetOrderStatusArgs` if defined separately), `from cyberdelta.core.models import Order`
    *   **Change Signature:**
        ```python
        @abstractmethod
        async def get_order_status(self, args: GetOrderArgs) -> Order | None: # Using GetOrderArgs
            # Note: HyperliquidAPI.get_order_status currently returns Order, not Order | None
            # But to align with get_order and potential for not found, Order | None is safer.
            raise NotImplementedError
        ```

3.  **Refactor `BackpackTradingService.get_order_status`:**
    *   **File:** `cyberdelta/apis/backpack/services/bp_trading_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetOrderArgs`
    *   **Change Signature:** `async def get_order_status(self, args: GetOrderArgs) -> Order:` (Keep `Order` if Backpack guarantees it or raises ORDER_NOT_FOUND)
    *   **Logic:** Remove individual parameter validation. Access parameters from `args`. Update endpoint path construction and call to `self._response_handler.handle_get_order_status_response`. Ensure `args.symbol` is provided as Backpack's API requires it.

4.  **Refactor `HyperliquidTradingService.get_order_status`:**
    *   **File:** `cyberdelta/apis/hyperliquid/services/hl_trading_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetOrderArgs`
    *   **Change Signature:** `async def get_order_status(self, args: GetOrderArgs) -> Order:` (HL currently raises if not found)
    *   **Logic:** This method currently calls `self.get_order`. Update it to:
        ```python
        async def get_order_status(self, args: GetOrderArgs) -> Order:
            order = await self.get_order(args=args) # Pass the args object
            if order is None:
                identifier = args.client_order_id if not args.order_id and args.client_order_id else args.order_id
                raise APIError(
                    f"Order {identifier} for symbol {args.symbol} not found on {self._exchange_name}.",
                    code=APIErrorCode.ORDER_NOT_FOUND.value,
                )
            return order
        ```

5.  **Update `BackpackAPI.get_order_status`:**
    *   **File:** `cyberdelta/apis/backpack/bp_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetOrderArgs`
    *   **Change Signature:** `async def get_order_status(self, args: GetOrderArgs) -> Order:`
    *   **Logic:** Call `self.trading_service.get_order_status(args=args)`.

6.  **Update `HyperliquidAPI.get_order_status`:**
    *   **File:** `cyberdelta/apis/hyperliquid/hl_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetOrderArgs`
    *   **Change Signature:** `async def get_order_status(self, args: GetOrderArgs) -> Order | None:` (Aligning with ExchangeAPI)
    *   **Logic:** Call `self.trading_service.get_order_status(args=args)`.

7.  **Update Call Sites:**
    *   Locate all calls to `get_order_status`.
    *   Modify them to instantiate and pass `GetOrderArgs` (or `GetOrderStatusArgs`).

8.  **Testing Requirements:**
    *   If `GetOrderStatusArgs` is defined separately, add unit tests for it. Otherwise, rely on `GetOrderArgs` tests.
    *   Update service and API unit tests for `get_order_status`.
    *   Ensure integration tests pass.

9.  **Static Analysis and Reporting:**
    *   Run static analysis tools and report results.

---

This set of prompts targets `get_order` and `get_order_status`. The key consideration for Angel will be whether to create a distinct `GetOrderStatusArgs` or reuse/alias `GetOrderArgs`. Given their similarity, reusing `GetOrderArgs` is probably the most efficient approach unless a clear functional difference in parameters emerges.


----



**Prompt 3 for AI Coder (Angel): Refactor `BackpackAPI.get_historical_funding_rates` Inputs with Pydantic Model**

**Project:** CyberDeltaEngine
**Context:**
The `BackpackAPI.get_historical_funding_rates` method currently takes several optional parameters for fetching historical funding rates (`symbol`, `start_time`, `end_time`, `limit`). Encapsulating these in a Pydantic model will improve the interface and allow for centralized validation.

**Goal:**
1.  Define a `GetHistoricalFundingRatesArgs` Pydantic model in `cyberdelta/apis/models/service_args_models.py`.
2.  Refactor the `BackpackAPI.get_historical_funding_rates` method to accept this model.
3.  Ensure the underlying `BackpackMarketDataService.get_historical_funding_rates` method is also updated or can correctly consume these arguments.
4.  Update all call sites.

**Why:**
Provides a structured and validated way to pass filtering and pagination parameters for historical funding rate requests, enhancing clarity and robustness.

**What to do (Step-by-Step for `get_historical_funding_rates` in `BackpackAPI`):**

1.  **Define `GetHistoricalFundingRatesArgs` Pydantic Model:**
    *   **File:** `cyberdelta/apis/models/service_args_models.py`
    *   **Imports:**
        ```python
        from datetime import datetime
        from typing import Any # For Any in validators
        from pydantic import BaseModel, Field, model_validator, field_validator, ValidationInfo, ConfigDict
        from cyberdelta.utils.parsing import parse_datetime_utc, validate_str_field
        ```
    *   **Model Definition (`GetHistoricalFundingRatesArgs`):**
        ```python
        class GetHistoricalFundingRatesArgs(BaseModel):
            model_config = ConfigDict(extra="forbid", validate_assignment=True)

            symbol: str # Symbol is required for this endpoint on Backpack
            start_time: datetime | None = Field(default=None)
            end_time: datetime | None = Field(default=None)
            limit: int | None = Field(default=None, gt=0)

            @field_validator("symbol", mode="before")
            @classmethod
            def validate_symbol_str(cls, v: Any, info: ValidationInfo) -> str:
                return validate_str_field(v, field_name=str(info.field_name), max_length=64, allow_empty=False)

            @field_validator("start_time", "end_time", mode="before")
            @classmethod
            def parse_optional_datetime_utc(cls, v: Any, info: ValidationInfo) -> datetime | None:
                if v is None:
                    return None
                return parse_datetime_utc(v, field_name=str(info.field_name))

            @field_validator("limit", mode="before")
            @classmethod
            def parse_optional_positive_int(cls, v: Any, info: ValidationInfo) -> int | None:
                if v is None:
                    return None
                if not isinstance(v, int | str | float):
                    raise ValueError(f"Field '{str(info.field_name)}' must be an integer or convertible.")
                try:
                    int_val = int(v)
                    return int_val
                except ValueError as e:
                    raise ValueError(f"Field '{str(info.field_name)}' could not be converted to int: {v}") from e

            @model_validator(mode="after")
            def check_time_range_logic(self) -> "GetHistoricalFundingRatesArgs":
                if self.start_time and self.end_time and self.start_time >= self.end_time:
                    raise ValueError("start_time must be before end_time if both are provided.")
                return self
        ```

2.  **Refactor `ExchangeAPI.get_historical_funding_rates` (Abstract Method):**
    *   **File:** `cyberdelta/apis/base/exchange_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetHistoricalFundingRatesArgs` (create if not exists), `from cyberdelta.core.models import FundingRate`
    *   **Change Signature:**
        ```python
        @abstractmethod
        async def get_historical_funding_rates(self, args: GetHistoricalFundingRatesArgs) -> list[FundingRate]:
            raise NotImplementedError
        ```
        *Note: HyperliquidAPI's current signature differs (`start_time` is not optional). This will need to be reconciled. The `GetHistoricalFundingRatesArgs` model makes `start_time` optional, so Hyperliquid's service/API will need to handle if it's `None` (e.g., raise error, use default).*

3.  **Refactor `BackpackMarketDataService.get_historical_funding_rates`:**
    *   **File:** `cyberdelta/apis/backpack/services/bp_market_data_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetHistoricalFundingRatesArgs`
    *   **Change Signature:** `async def get_historical_funding_rates(self, args: GetHistoricalFundingRatesArgs) -> list[FundingRate]:`
    *   **Logic:** Remove individual parameter validation. Access parameters from `args`. Update call to `self._request_builder.build_get_historical_funding_rates_params`.

4.  **Refactor `HyperliquidMarketDataService.get_historical_funding_rates`:**
    *   **File:** `cyberdelta/apis/hyperliquid/services/hl_market_data_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetHistoricalFundingRatesArgs`
    *   **Change Signature:** `async def get_historical_funding_rates(self, args: GetHistoricalFundingRatesArgs) -> list[FundingRate]:`
    *   **Logic:**
        *   Remove individual parameter validation. Access parameters from `args`.
        *   **Hyperliquid Specific:** Hyperliquid's builder requires `start_time_ms` (and `symbol`). The service must ensure `args.start_time` is provided (or raise an error/use a default if `GetHistoricalFundingRatesArgs` keeps it optional). Convert `args.start_time` and `args.end_time` to milliseconds.

5.  **Update `BackpackAPI.get_historical_funding_rates`:**
    *   **File:** `cyberdelta/apis/backpack/bp_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetHistoricalFundingRatesArgs`
    *   **Change Signature:** `async def get_historical_funding_rates(self, args: GetHistoricalFundingRatesArgs) -> list[FundingRate]:`
    *   **Logic:** Call `self.market_data_service.get_historical_funding_rates(args=args)`.

6.  **Update `HyperliquidAPI.get_historical_funding_rates`:**
    *   **File:** `cyberdelta/apis/hyperliquid/hl_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetHistoricalFundingRatesArgs`
    *   **Change Signature:** `async def get_historical_funding_rates(self, args: GetHistoricalFundingRatesArgs) -> list[FundingRate]:`
    *   **Logic:** Call `self.market_data_service.get_historical_funding_rates(args=args)`.

7.  **Update Call Sites:**
    *   Locate all calls to `get_historical_funding_rates`.
    *   Modify them to instantiate and pass `GetHistoricalFundingRatesArgs`.

8.  **Testing Requirements:**
    *   Add unit tests for `GetHistoricalFundingRatesArgs` model validation.
    *   Update service and API unit tests for `get_historical_funding_rates`.
    *   Address any discrepancies in required parameters between Backpack and Hyperliquid (e.g., `start_time` being optional in `GetHistoricalFundingRatesArgs` vs. required by Hyperliquid's builder). The service layer for Hyperliquid must handle this, potentially raising an error if `args.start_time` is `None`.

9.  **Static Analysis and Reporting:**
    *   Run static analysis tools and report results.

