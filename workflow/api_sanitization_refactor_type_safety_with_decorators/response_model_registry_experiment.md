# Response Model Registry Experiment

## Concept

Create a **lookup table** that maps API endpoints to their expected response models, enabling automatic validation without breaking exchange agnosticism.

## Implementation

### 1. Registry Structure

```python
# cyberdelta/apis/backpack/bp_response_registry.py
from typing import Type, Any, get_args, get_origin
from cyberdelta.apis.backpack.models import *

class BackpackResponseRegistry:
    """Maps Backpack endpoints to their expected response models."""

    ENDPOINT_MODELS = {
        # Market Data
        "/api/v1/ticker": BackpackRawTicker,
        "/api/v1/depth": BackpackRawOrderBook,
        "/api/v1/trades": list[BackpackRawTrade],
        "/api/v1/klines": list[BackpackRawKline],

        # Account Data
        "/api/v1/capital": dict[str, BackpackRawBalance],
        "/api/v1/order": list[BackpackRawOrder],
        "/api/v1/position": list[BackpackRawPosition],
        "/api/v1/account": BackpackRawAccountSummary,

        # Trading
        "/wapi/v1/order": BackpackRawOrder,  # Place order response
        "/wapi/v1/order/cancel": BackpackRawOrder,  # Cancel response

        # Operational
        "/wapi/v1/withdraw": BackpackRawWithdrawalResponse,
        "/api/v1/funding": list[BackpackRawFundingRate],
    }

    @classmethod
    def get_response_model(cls, endpoint: str) -> Type[Any]:
        """Get expected response model for endpoint."""
        if endpoint not in cls.ENDPOINT_MODELS:
            raise ValueError(f"Unknown endpoint: {endpoint}")
        return cls.ENDPOINT_MODELS[endpoint]

    @classmethod
    def validate_response(cls, endpoint: str, raw_data: Any) -> Any:
        """Validate raw response against expected model."""
        model = cls.get_response_model(endpoint)

        # Handle different model types
        if get_origin(model) is dict:
            # dict[str, SomeModel] response
            if not isinstance(raw_data, dict):
                raise ValueError(f"Expected dict for {endpoint}, got {type(raw_data)}")

            value_model = get_args(model)[1]  # Get SomeModel from dict[str, SomeModel]
            return {k: value_model.model_validate(v) for k, v in raw_data.items()}

        elif get_origin(model) is list:
            # list[SomeModel] response
            if not isinstance(raw_data, list):
                raise ValueError(f"Expected list for {endpoint}, got {type(raw_data)}")

            item_model = get_args(model)[0]  # Get SomeModel from list[SomeModel]
            return [item_model.model_validate(item) for item in raw_data]

        else:
            # Single model response
            return model.model_validate(raw_data)

    @classmethod
    def is_known_endpoint(cls, endpoint: str) -> bool:
        """Check if endpoint is registered."""
        return endpoint in cls.ENDPOINT_MODELS

    @classmethod
    def get_all_endpoints(cls) -> list[str]:
        """Get all registered endpoints."""
        return list(cls.ENDPOINT_MODELS.keys())
```

### 2. Hyperliquid Registry

```python
# cyberdelta/apis/hyperliquid/hl_response_registry.py
from typing import Type, Any
from cyberdelta.apis.hyperliquid.models import *

class HyperliquidResponseRegistry:
    """Maps Hyperliquid endpoints to their expected response models."""

    ENDPOINT_MODELS = {
        # Info endpoints (by request type)
        "/info:user_state": HyperliquidRawUserStateResponse,
        "/info:meta": HyperliquidRawMetaAndAssetCtxsResponse,
        "/info:all_mids": HyperliquidRawAllMids,
        "/info:l2_book": HyperliquidRawL2Book,
        "/info:open_orders": HyperliquidRawOpenOrdersResponse,
        "/info:user_fills": HyperliquidRawUserFillsResponse,
        "/info:funding_history": list[HyperliquidRawFundingHistoryInfo],
        "/info:candles": list[HyperliquidRawCandleSnapshot],

        # Exchange endpoints
        "/exchange:place_order": HyperliquidRawExchangeResponse,
        "/exchange:cancel_order": HyperliquidRawExchangeResponse,
        "/exchange:cancel_all": HyperliquidRawExchangeResponse,
        "/exchange:modify_order": HyperliquidRawExchangeResponse,
    }

    @classmethod
    def get_info_endpoint_key(cls, request_type: str) -> str:
        """Build endpoint key for /info requests."""
        return f"/info:{request_type}"

    @classmethod
    def get_exchange_endpoint_key(cls, action_type: str) -> str:
        """Build endpoint key for /exchange requests."""
        return f"/exchange:{action_type}"

    @classmethod
    def validate_info_response(cls, request_type: str, raw_data: Any) -> Any:
        """Validate /info response based on request type."""
        endpoint_key = cls.get_info_endpoint_key(request_type)
        return cls.validate_response(endpoint_key, raw_data)

    @classmethod
    def validate_exchange_response(cls, action_type: str, raw_data: Any) -> Any:
        """Validate /exchange response based on action type."""
        endpoint_key = cls.get_exchange_endpoint_key(action_type)
        return cls.validate_response(endpoint_key, raw_data)

    # Inherit other methods from base implementation
    @classmethod
    def get_response_model(cls, endpoint: str) -> Type[Any]:
        if endpoint not in cls.ENDPOINT_MODELS:
            raise ValueError(f"Unknown endpoint: {endpoint}")
        return cls.ENDPOINT_MODELS[endpoint]

    @classmethod
    def validate_response(cls, endpoint: str, raw_data: Any) -> Any:
        # Same logic as BackpackResponseRegistry
        model = cls.get_response_model(endpoint)

        if get_origin(model) is dict:
            if not isinstance(raw_data, dict):
                raise ValueError(f"Expected dict for {endpoint}")
            value_model = get_args(model)[1]
            return {k: value_model.model_validate(v) for k, v in raw_data.items()}
        elif get_origin(model) is list:
            if not isinstance(raw_data, list):
                raise ValueError(f"Expected list for {endpoint}")
            item_model = get_args(model)[0]
            return [item_model.model_validate(item) for item in raw_data]
        else:
            return model.model_validate(raw_data)
```

### 3. Service Integration

```python
# cyberdelta/apis/backpack/services/bp_account_service.py
from cyberdelta.apis.backpack.bp_response_registry import BackpackResponseRegistry

class BackpackAccountService:
    # Keep existing methods unchanged for now
    async def get_balances(self) -> dict[str, SpotBalance]:
        # Current implementation unchanged
        pass

    # NEW: Experimental method using registry
    async def get_balances_registry(self) -> dict[str, SpotBalance]:
        """Experiment: Use registry for validation."""
        params = self._request_builder.build_get_balances_params()
        raw_data, status_code, headers = await self._http_client_requester(
            endpoint="/api/v1/capital",
            params=params.model_dump(),
            is_signed=True
        )

        if raw_data is None:
            raise APIError(f"No data received for balances")

        # Registry validates based on endpoint - SKIPS ResponseHandler
        raw_balances = BackpackResponseRegistry.validate_response(
            endpoint="/api/v1/capital",
            raw_data=raw_data
        )

        # Transform to domain models (same as before)
        return {asset: self._mapper.transform_raw_balance_to_internal(asset, bal)
                for asset, bal in raw_balances.items()}

    async def get_positions_registry(self, symbol: str | None = None) -> list[DerivativePosition]:
        """Experiment: Registry validation for positions."""
        params = self._request_builder.build_get_positions_params(symbol)
        raw_data, status_code, headers = await self._http_client_requester(
            endpoint="/api/v1/position",
            params=params.model_dump(),
            is_signed=True
        )

        if raw_data is None:
            if status_code == 404:
                return []  # No positions
            raise APIError(f"No data received for positions")

        # Registry validation
        raw_positions = BackpackResponseRegistry.validate_response(
            endpoint="/api/v1/position",
            raw_data=raw_data
        )

        # Transform to domain models
        return [self._mapper.transform_raw_position_to_internal(pos)
                for pos in raw_positions]
```

### 4. Hyperliquid Service Integration

```python
# cyberdelta/apis/hyperliquid/services/hl_account_service.py
from cyberdelta.apis.hyperliquid.hl_response_registry import HyperliquidResponseRegistry

class HyperliquidAccountService:
    async def get_user_state_registry(self, args: GetUserStateArgs) -> MarginAccountSummary:
        """Experiment: Registry validation for user state."""
        request_payload = self._request_builder.build_user_state_request(
            user_address=args.user_address
        )

        raw_data, status_code, headers = await self._http_client_requester(
            method="POST",
            endpoint="/info",
            data=request_payload.model_dump(),
            is_signed=False,
        )

        if raw_data is None:
            raise APIError("No user state data received")

        # Registry validation with request type context
        raw_user_state = HyperliquidResponseRegistry.validate_info_response(
            request_type="user_state",
            raw_data=raw_data
        )

        # Transform to domain model
        return self._account_mapper.transform_raw_user_state_to_margin_summary(
            raw_user_state, args.user_address
        )
```

## Benefits

### 1. **Centralized Documentation**
- All endpoint → model mappings in one place
- Easy to see what each endpoint should return
- Self-documenting API contracts

### 2. **Type Safety Without Architecture Changes**
- HttpClient stays exchange-agnostic
- Services can opt-in to registry validation
- No breaking changes to existing code

### 3. **Intelligent Type Handling**
- Automatically handles `dict[str, Model]` responses
- Automatically handles `list[Model]` responses
- Single validation point for complex types

### 4. **Exchange-Specific Customization**
- Each exchange has its own registry
- Can handle exchange-specific patterns (like Hyperliquid's `/info` types)
- Extensible for new exchanges

## Potential Issues

### 1. **Manual Maintenance**
- Need to update registry when adding new endpoints
- Risk of forgetting to register new endpoints

### 2. **Runtime Errors**
- Typos in endpoint names only caught at runtime
- No compile-time verification of endpoint existence

### 3. **Complex Type Parsing**
- Generic type introspection (`get_origin`, `get_args`) can be fragile
- May break with future Python versions

### 4. **Inconsistent Usage**
- Services have both registry and non-registry methods
- Could lead to confusion about which approach to use

## Testing Strategy

```python
# tests/apis/backpack/test_bp_response_registry.py

def test_backpack_registry_ticker():
    """Test registry validation for ticker endpoint."""
    mock_ticker_data = {
        "symbol": "BTC_USDC",
        "lastPrice": "45000.00",
        "volume": "123.45",
        # ... complete ticker data
    }

    result = BackpackResponseRegistry.validate_response(
        endpoint="/api/v1/ticker",
        raw_data=mock_ticker_data
    )

    assert isinstance(result, BackpackRawTicker)
    assert result.symbol == "BTC_USDC"
    assert result.last_price == "45000.00"

def test_backpack_registry_balances():
    """Test registry validation for balances endpoint."""
    mock_balances_data = {
        "BTC": {"available": "1.0", "locked": "0.0"},
        "USDC": {"available": "45000.0", "locked": "1000.0"}
    }

    result = BackpackResponseRegistry.validate_response(
        endpoint="/api/v1/capital",
        raw_data=mock_balances_data
    )

    assert isinstance(result, dict)
    assert isinstance(result["BTC"], BackpackRawBalance)
    assert isinstance(result["USDC"], BackpackRawBalance)

def test_backpack_registry_unknown_endpoint():
    """Test registry error handling for unknown endpoint."""
    with pytest.raises(ValueError, match="Unknown endpoint"):
        BackpackResponseRegistry.validate_response(
            endpoint="/api/v1/unknown",
            raw_data={}
        )

def test_hyperliquid_registry_info_types():
    """Test Hyperliquid registry with info request types."""
    mock_user_state = {
        "marginSummary": {"accountValue": "1000.0"},
        "assetPositions": []
    }

    result = HyperliquidResponseRegistry.validate_info_response(
        request_type="user_state",
        raw_data=mock_user_state
    )

    assert isinstance(result, HyperliquidRawUserStateResponse)
```

## Future Enhancements

### 1. **Auto-Generated Registries**
```python
# Could scan Raw models and auto-generate registries based on naming patterns
def generate_registry_from_models():
    """Auto-generate registry from existing Raw models."""
    pass
```

### 2. **Endpoint Validation Decorators**
```python
@validate_with_registry
async def get_ticker(self, symbol: str) -> Ticker:
    # Decorator automatically validates response using registry
    pass
```

### 3. **Registry Merging**
```python
# Combine multiple exchange registries for multi-exchange clients
combined_registry = BackpackResponseRegistry + HyperliquidResponseRegistry
```

## Conclusion

The Response Model Registry experiment provides:

- ✅ **Type safety** without architectural changes
- ✅ **Exchange agnosticism** maintained
- ✅ **Centralized documentation** of API contracts
- ✅ **Gradual adoption** possible
- ✅ **Exchange-specific customization** supported

This approach could serve as a stepping stone toward more type-safe API responses while preserving the current clean architecture.

Worth experimenting with a few endpoints to see how it feels in practice!
