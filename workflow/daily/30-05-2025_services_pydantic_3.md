
**Prompt 1 for AI Coder (Angel): Refactor `cancel_order` Service Method Inputs with Pydantic Model**

**Project:** CyberDeltaEngine
**Context:**
We are standardizing service method inputs using Pydantic models. The `cancel_order` method, while often having fewer parameters than `place_order`, still involves an `order_id` and an optional `symbol`, and sometimes a `client_order_id`. Encapsulating these in a Pydantic model will provide consistent validation and a cleaner interface.

**Goal:**
1.  Define a `CancelOrderArgs` Pydantic model in `cyberdelta/apis/models/service_args_models.py`.
2.  Refactor the abstract `ExchangeAPI.cancel_order` method.
3.  Refactor concrete implementations in `BackpackAPI`, `HyperliquidAPI`, and their trading services.
4.  Update all call sites.

**Why:**
Using a Pydantic model for `cancel_order` arguments ensures `order_id` is always a valid non-empty string and handles optional parameters like `symbol` and `client_order_id` gracefully with validation.

**What to do (Step-by-Step for `cancel_order`):**

1.  **Define `CancelOrderArgs` Pydantic Model:**
    *   **File:** `cyberdelta/apis/models/service_args_models.py`
    *   **Imports:**
        ```python
        from pydantic import BaseModel, Field, model_validator, field_validator, ValidationInfo, ConfigDict
        from cyberdelta.utils.parsing import validate_str_field # If used in validators
        ```
    *   **Model Definition (`CancelOrderArgs`):**
        ```python
        class CancelOrderArgs(BaseModel):
            model_config = ConfigDict(extra="forbid", validate_assignment=True)

            order_id: str # Usually the exchange-generated order ID
            symbol: str | None = Field(default=None) # Often required by exchanges
            client_order_id: str | None = Field(default=None) # Alternative identifier

            @field_validator("order_id", "symbol", "client_order_id", mode="before")
            @classmethod
            def validate_strings(cls, v: Any, info: ValidationInfo) -> str | None:
                field_name = str(info.field_name)
                is_required = field_name == "order_id" # order_id is always required

                if v is None:
                    if is_required:
                        raise ValueError(f"Field '{field_name}' is required.")
                    return None # For optional fields

                # Assuming generic string validation, max_length can be adjusted
                # allow_empty should be False for IDs and symbols if they are provided
                return validate_str_field(v, field_name=field_name, max_length=128, allow_empty=False)

            @model_validator(mode="after")
            def check_identifiers_logic(self) -> "CancelOrderArgs":
                # Example: Some exchanges might require symbol if not using client_order_id,
                # or only one of order_id/client_order_id.
                # For Backpack, 'symbol' is required, and one of 'orderId' or 'clientId'.
                # For Hyperliquid, 'asset' (derived from symbol) and 'oid' (order_id) are needed.
                # This generic model ensures order_id is present. Exchange-specific services
                # will need to ensure `symbol` is also provided if their RequestBuilder requires it.
                if self.symbol is None:
                     # Depending on exchange specifics, this might be an error for some.
                     # For now, allow symbol to be optional in the generic model.
                     # The service/builder for a specific exchange will enforce if it's needed.
                     pass
                return self
        ```

2.  **Refactor `ExchangeAPI.cancel_order` (Abstract Method):**
    *   **File:** `cyberdelta/apis/base/exchange_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import CancelOrderArgs`
    *   **Change Signature:**
        ```python
        @abstractmethod
        async def cancel_order(self, args: CancelOrderArgs) -> bool: # Returns bool
            raise NotImplementedError
        ```

3.  **Refactor `BackpackTradingService.cancel_order`:**
    *   **File:** `cyberdelta/apis/backpack/services/bp_trading_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import CancelOrderArgs`
    *   **Change Signature:** `async def cancel_order(self, args: CancelOrderArgs) -> bool:`
    *   **Logic:** Remove individual parameter validation. Access parameters from `args`. Update call to `self._request_builder.build_cancel_order_payload`. Backpack requires `symbol`. The `CancelOrderArgs` model validator or the service method itself should ensure `args.symbol` is not `None` before calling the builder.

4.  **Refactor `HyperliquidTradingService.cancel_order`:**
    *   **File:** `cyberdelta/apis/hyperliquid/services/hl_trading_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import CancelOrderArgs`
    *   **Change Signature:** `async def cancel_order(self, args: CancelOrderArgs) -> bool:`
    *   **Logic:** Remove individual parameter validation. Access parameters from `args`. Convert `args.order_id` to `int` for Hyperliquid. Get `asset_index` from `args.symbol`. Update call to `self._request_builder.build_cancel_order_payload`.

5.  **Update `BackpackAPI.cancel_order`:**
    *   **File:** `cyberdelta/apis/backpack/bp_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import CancelOrderArgs`
    *   **Change Signature:** `async def cancel_order(self, args: CancelOrderArgs) -> bool:`
    *   **Logic:** Call `self.trading_service.cancel_order(args=args)`.

6.  **Update `HyperliquidAPI.cancel_order`:**
    *   **File:** `cyberdelta/apis/hyperliquid/hl_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import CancelOrderArgs`
    *   **Change Signature:** `async def cancel_order(self, args: CancelOrderArgs) -> bool:`
    *   **Logic:** Call `self.trading_service.cancel_order(args=args)`.

7.  **Update Call Sites:**
    *   Locate all calls to `cancel_order`.
    *   Modify them to instantiate and pass `CancelOrderArgs`.

8.  **Testing Requirements:**
    *   Add unit tests for `CancelOrderArgs` model validation.
    *   Update service and API unit tests for `cancel_order`.
    *   Ensure integration tests involving order cancellation are updated and pass.

9.  **Static Analysis and Reporting:**
    *   Run static analysis tools and report results.

---
**Prompt 2 for AI Coder (Angel): Refactor `get_funding_rates` Service Method Inputs with Pydantic Model**

**Project:** CyberDeltaEngine
**Context:**
The `get_funding_rates` method typically takes an optional list of symbols. While simple, using a Pydantic model can enforce that if symbols are provided, it's a list of valid, non-empty strings, and that the list itself is not empty if provided.

**Goal:**
1.  Define a `GetFundingRatesArgs` Pydantic model in `cyberdelta/apis/models/service_args_models.py`.
2.  Refactor the abstract `ExchangeAPI.get_funding_rates` method.
3.  Refactor concrete implementations in `BackpackAPI`, `HyperliquidAPI`, and their market data services.
4.  Update all call sites.

**Why:**
This ensures consistency in how symbol lists are handled and validated for fetching funding rates, improving robustness for this common operation.

**What to do (Step-by-Step for `get_funding_rates`):**

1.  **Define `GetFundingRatesArgs` Pydantic Model:**
    *   **File:** `cyberdelta/apis/models/service_args_models.py`
    *   **Imports:**
        ```python
        from typing import List # For List, or use list directly
        from pydantic import BaseModel, Field, field_validator, ValidationInfo, ConfigDict
        from cyberdelta.utils.parsing import validate_str_field
        ```
    *   **Model Definition (`GetFundingRatesArgs`):**
        ```python
        class GetFundingRatesArgs(BaseModel):
            model_config = ConfigDict(extra="forbid", validate_assignment=True)

            symbols: list[str] | None = Field(default=None) # List of symbols, or None for all

            @field_validator("symbols", mode="before")
            @classmethod
            def validate_symbols_list(cls, v: Any, info: ValidationInfo) -> list[str] | None:
                if v is None:
                    return None # Allowed
                if not isinstance(v, list):
                    raise ValueError(f"Field '{str(info.field_name)}' must be a list of strings or None.")
                if not v: # Empty list is passed through, service must decide if "all" or error
                    return []

                validated_symbols: list[str] = []
                for i, item in enumerate(v):
                    # Ensure item is a non-empty string
                    item_str = validate_str_field(item, field_name=f"{str(info.field_name)}[{i}]", max_length=64, allow_empty=False)
                    validated_symbols.append(item_str)
                return validated_symbols
        ```

2.  **Refactor `ExchangeAPI.get_funding_rates` (Abstract Method):**
    *   **File:** `cyberdelta/apis/base/exchange_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetFundingRatesArgs`, `from cyberdelta.core.models import FundingRate`
    *   **Change Signature:**
        ```python
        @abstractmethod
        async def get_funding_rates(self, args: GetFundingRatesArgs) -> list[FundingRate]:
            raise NotImplementedError
        ```

3.  **Refactor `BackpackMarketDataService.get_funding_rates`:**
    *   **File:** `cyberdelta/apis/backpack/services/bp_market_data_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetFundingRatesArgs`
    *   **Change Signature:** `async def get_funding_rates(self, args: GetFundingRatesArgs) -> list[FundingRate]:`
    *   **Logic:** Remove individual parameter validation. Access `args.symbols`.
        *   **Backpack Specific:** If `args.symbols` is `None` or empty, the service should fetch for all supported funding rate markets or raise an error if Backpack requires specific symbols. If symbols are provided, iterate and call `self.get_funding_rate(symbol)` for each.

4.  **Refactor `HyperliquidMarketDataService.get_funding_rates`:**
    *   **File:** `cyberdelta/apis/hyperliquid/services/hl_market_data_service.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetFundingRatesArgs`
    *   **Change Signature:** `async def get_funding_rates(self, args: GetFundingRatesArgs) -> list[FundingRate]:`
    *   **Logic:** Remove individual parameter validation. Access `args.symbols`.
        *   **Hyperliquid Specific:** Hyperliquid's `get_all_asset_contexts_raw` fetches data for all symbols. The service then filters by `args.symbols` if provided, or processes all if `args.symbols` is `None` or empty.

5.  **Update `BackpackAPI.get_funding_rates`:**
    *   **File:** `cyberdelta/apis/backpack/bp_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetFundingRatesArgs`
    *   **Change Signature:** `async def get_funding_rates(self, args: GetFundingRatesArgs) -> list[FundingRate]:`
    *   **Logic:** Call `self.market_data_service.get_funding_rates(args=args)`.

6.  **Update `HyperliquidAPI.get_funding_rates`:**
    *   **File:** `cyberdelta/apis/hyperliquid/hl_api.py`
    *   **Imports:** `from cyberdelta.apis.models.service_args_models import GetFundingRatesArgs`
    *   **Change Signature:** `async def get_funding_rates(self, args: GetFundingRatesArgs) -> list[FundingRate]:`
    *   **Logic:** Call `self.market_data_service.get_funding_rates(args=args)`.

7.  **Update Call Sites:**
    *   Locate all calls to `get_funding_rates`.
    *   Modify them to instantiate and pass `GetFundingRatesArgs`. E.g., `GetFundingRatesArgs(symbols=["BTC-PERP", "ETH-PERP"])` or `GetFundingRatesArgs()` for all.

8.  **Testing Requirements:**
    *   Add unit tests for `GetFundingRatesArgs` model validation.
    *   Update service and API unit tests for `get_funding_rates`.
    *   Ensure integration tests involving funding rate fetching are updated and pass.

9.  **Static Analysis and Reporting:**
    *   Run static analysis tools and report results.
