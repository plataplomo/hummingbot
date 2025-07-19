# Proper Spot Mappings Implementation Plan

## Current Understanding

1. **Environment Detection**:
   - Environment is determined by `exchange_config.environment_type` (EnvironmentType enum)
   - NOT by checking for "testnet" in strings
   - The asset indexer doesn't currently have access to environment_type

2. **Asset Indexer Initialization**:
   - Created in `hl_api.py` with only exchange_name="hyperliquid"
   - Doesn't receive the exchange_config or environment_type
   - Cannot properly determine testnet vs mainnet

## Proposed Solution

### Option 1: Pass environment_type to Asset Indexer (Recommended)

1. Update `HyperliquidAssetIndexResolver.__init__` to accept `environment_type`
2. Update `hl_api.py` to pass `environment_type` when creating indexer
3. Use environment_type to select appropriate spot mappings

### Option 2: Pass entire exchange_config (More flexible but heavier)

1. Update asset indexer to accept exchange_config
2. Extract environment_type from config
3. More flexible for future needs

## Implementation Steps (Option 1)

### Step 1: Update Asset Indexer Constructor

```python
def __init__(
    self,
    requester: ...,
    response_handler: ...,
    request_builder: ...,
    exchange_name_for_log: str = "hyperliquid_asset_indexer",
    environment_type: EnvironmentType | None = None,  # New parameter
) -> None:
    # ... existing init code ...
    self._environment_type = environment_type or EnvironmentType.MAINNET
```

### Step 2: Update hl_api.py

```python
self._asset_indexer = HyperliquidAssetIndexResolver(
    requester=self._request,
    response_handler=market_data_response_handler,
    request_builder=market_data_request_builder,
    exchange_name_for_log=self.exchange_name,
    environment_type=exchange_config.environment_type,  # Pass environment
)
```

### Step 3: Update _resolve_spot_symbol_direct

```python
def _resolve_spot_symbol_direct(self, symbol: str) -> int | None:
    """Resolve spot symbols that have direct mappings."""
    # Handle @N format
    if symbol.startswith("@") and symbol[1:].isdigit():
        return int(symbol[1:])

    # Use environment-specific mappings
    if self._environment_type == EnvironmentType.TESTNET:
        # Common testnet mappings
        testnet_mappings = {
            "PURR/USDC": 0,
            "TEST/USDC": 2,
            "BTC/USDC": 69,
            # Add more as needed
        }
        return testnet_mappings.get(symbol)
    else:
        # Mainnet mappings (to be added)
        return None
```

## Benefits

1. **Proper Architecture**: Uses existing environment detection mechanism
2. **Type Safe**: Uses EnvironmentType enum
3. **Minimal Changes**: Only touches 2 files
4. **Testable**: Can mock environment_type in tests
5. **Future Proof**: Easy to add mainnet mappings later

## Testing

1. Update existing asset indexer tests to pass environment_type
2. Add tests for spot symbol resolution with different environments
3. Ensure backward compatibility (default to mainnet if not specified)
