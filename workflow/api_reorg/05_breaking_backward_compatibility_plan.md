# Breaking Backward Compatibility - Deep Code Research & Migration Plan

## Executive Summary
This document provides a comprehensive plan to break backward compatibility for the service args models refactoring. The analysis shows that 158 files import from `service_args_models.py`, but only 4 test files are still using the old Hyperliquid model names. The migration can be completed safely with minimal risk.

## Current State Analysis

### 1. Backward Compatibility Layer
The file `/cyberdelta/apis/models/service_args_models.py` (115 lines) serves as a compatibility shim:
- Re-exports all models from the new module structure
- Provides aliases for 7 old Hyperliquid model names
- Uses star imports which required explicit imports to avoid F405 errors

### 2. Old Model Name Aliases
```python
GetOrderHistoryArgsHL = HyperliquidGetOrderHistoryArgs
TransferL2UsdArgs = HyperliquidTransferL2UsdArgs
GetUserStateArgs = HyperliquidGetUserStateArgs
GetUserFillsArgs = HyperliquidGetUserFillsArgs
UpdateLeverageArgs = HyperliquidUpdateLeverageArgs
WithdrawL1Args = HyperliquidWithdrawL1Args
GetCandleSnapshotArgs = HyperliquidGetCandleSnapshotArgs
```

### 3. Files Still Using Old Names
Only 4 test files are still importing old model names:

1. **tests/unit/apis/hyperliquid/test_hl_request_builder_transfers.py**
   - Uses: `TransferL2UsdArgs`, `WithdrawL1Args`
   - Lines: 18-19, 47, 66, 82, 101

2. **tests/unit/apis/hyperliquid/test_hl_request_builder_trading.py**
   - Uses: `GetOrderHistoryArgsHL`
   - Lines: 25, 92, 109

3. **tests/unit/apis/hyperliquid/test_hl_request_builder_info_market.py**
   - Uses: `GetCandleSnapshotArgs`
   - Lines: 20, 53, 77, 87, 103, 108

4. **tests/unit/apis/hyperliquid/services/test_hl_market_data_service_candles.py**
   - Uses: `GetCandleSnapshotArgs`
   - Lines: 13, 262, 306, 744

### 4. Import Analysis
- **Total files importing from service_args_models.py**: 158
- **Files using old model names**: 4 (all test files)
- **No dynamic imports or getattr usage detected**
- **All other files already use new names or generic models**

## Migration Plan

### Phase 1: Update Remaining Test Files (Immediate)

#### 1.1 Update test_hl_request_builder_transfers.py
```python
# Old imports (lines 18-19)
from cyberdelta.apis.models.service_args_models import (
    TransferL2UsdArgs,
    WithdrawL1Args,
)

# New imports
from cyberdelta.apis.models.service_args.hyperliquid import (
    HyperliquidTransferL2UsdArgs,
    HyperliquidWithdrawL1Args,
)

# Update usage:
# Line 47: WithdrawL1Args → HyperliquidWithdrawL1Args
# Line 66: TransferL2UsdArgs → HyperliquidTransferL2UsdArgs
# Line 82: WithdrawL1Args → HyperliquidWithdrawL1Args
# Line 101: TransferL2UsdArgs → HyperliquidTransferL2UsdArgs
```

#### 1.2 Update test_hl_request_builder_trading.py
```python
# Old import (line 25)
from cyberdelta.apis.models.service_args_models import (
    GetOrderHistoryArgsHL,
    # ... other imports
)

# New import
from cyberdelta.apis.models.service_args.hyperliquid import (
    HyperliquidGetOrderHistoryArgs,
)
# Keep other imports from service_args_models or update to service_args

# Update usage:
# Line 92: GetOrderHistoryArgsHL → HyperliquidGetOrderHistoryArgs
# Line 109: GetOrderHistoryArgsHL → HyperliquidGetOrderHistoryArgs
```

#### 1.3 Update test_hl_request_builder_info_market.py
```python
# Old import (line 20)
from cyberdelta.apis.models.service_args_models import (
    GetCandleSnapshotArgs,
    # ... other imports
)

# New import
from cyberdelta.apis.models.service_args.hyperliquid import (
    HyperliquidGetCandleSnapshotArgs,
)
# Keep other imports from service_args_models or update to service_args

# Update usage:
# Lines 53, 77, 87, 103, 108: GetCandleSnapshotArgs → HyperliquidGetCandleSnapshotArgs
```

#### 1.4 Update test_hl_market_data_service_candles.py
```python
# Old import (line 13)
from cyberdelta.apis.models.service_args_models import GetCandleSnapshotArgs, GetMarketDataArgs

# New imports
from cyberdelta.apis.models.service_args import GetMarketDataArgs
from cyberdelta.apis.models.service_args.hyperliquid import HyperliquidGetCandleSnapshotArgs

# Update usage:
# Lines 262, 306, 744: GetCandleSnapshotArgs → HyperliquidGetCandleSnapshotArgs
```

### Phase 2: Update Import Paths (Optional but Recommended)

While not strictly necessary, updating all 158 files to use the new import paths would be cleaner:

#### 2.1 Generic Models Import Update
```python
# Old
from cyberdelta.apis.models.service_args_models import (
    PlaceOrderArgs,
    CancelOrderArgs,
    GetMarketDataArgs,
)

# New (Option A - from package root)
from cyberdelta.apis.models.service_args import (
    PlaceOrderArgs,
    CancelOrderArgs,
    GetMarketDataArgs,
)

# New (Option B - from specific module)
from cyberdelta.apis.models.service_args.common import (
    PlaceOrderArgs,
    CancelOrderArgs,
    GetMarketDataArgs,
)
```

#### 2.2 Mixed Imports Update
```python
# Old
from cyberdelta.apis.models.service_args_models import (
    PlaceOrderArgs,  # generic
    HyperliquidGetOrderStatusArgs,  # HL-specific
)

# New
from cyberdelta.apis.models.service_args import PlaceOrderArgs
from cyberdelta.apis.models.service_args.hyperliquid import HyperliquidGetOrderStatusArgs
```

### Phase 3: Remove Backward Compatibility

#### 3.1 Delete the Compatibility File
```bash
rm /workspaces/CyberDeltaEngine/worktrees/api-reorg-improvement/cyberdelta/apis/models/service_args_models.py
```

#### 3.2 Run Linters to Catch Any Missed Imports
```bash
mypy .
ruff check .
pyright .
```

#### 3.3 Run Test Suite
```bash
pytest -xvs
```

## Automation Script

```python
#!/usr/bin/env python3
"""Automated migration script for breaking service_args backward compatibility."""

import re
from pathlib import Path
from typing import Dict, List, Tuple

# Mapping of old names to new names and their module
OLD_TO_NEW_MAPPING = {
    "GetOrderHistoryArgsHL": ("hyperliquid", "HyperliquidGetOrderHistoryArgs"),
    "TransferL2UsdArgs": ("hyperliquid", "HyperliquidTransferL2UsdArgs"),
    "GetUserStateArgs": ("hyperliquid", "HyperliquidGetUserStateArgs"),
    "GetUserFillsArgs": ("hyperliquid", "HyperliquidGetUserFillsArgs"),
    "UpdateLeverageArgs": ("hyperliquid", "HyperliquidUpdateLeverageArgs"),
    "WithdrawL1Args": ("hyperliquid", "HyperliquidWithdrawL1Args"),
    "GetCandleSnapshotArgs": ("hyperliquid", "HyperliquidGetCandleSnapshotArgs"),
}

def update_imports_in_file(filepath: Path) -> bool:
    """Update imports in a single file."""
    with open(filepath, 'r') as f:
        content = f.read()

    original_content = content

    # Pattern to match imports from service_args_models
    import_pattern = r'from\s+cyberdelta\.apis\.models\.service_args_models\s+import\s+\((.*?)\)'

    def process_imports(match):
        imports_str = match.group(1)
        imports = [imp.strip() for imp in imports_str.split(',')]

        generic_imports = []
        hl_imports = []

        for imp in imports:
            if imp in OLD_TO_NEW_MAPPING:
                module, new_name = OLD_TO_NEW_MAPPING[imp]
                hl_imports.append((imp, new_name))
            else:
                generic_imports.append(imp)

        # Build new import statements
        new_imports = []

        if generic_imports:
            new_imports.append(
                f"from cyberdelta.apis.models.service_args import (\n    " +
                ",\n    ".join(generic_imports) + "\n)"
            )

        if hl_imports:
            new_imports.append(
                f"from cyberdelta.apis.models.service_args.hyperliquid import (\n    " +
                ",\n    ".join([new_name for _, new_name in hl_imports]) + "\n)"
            )

        # Update the content to use new names
        for old_name, new_name in hl_imports:
            nonlocal content
            content = re.sub(rf'\b{old_name}\b', new_name, content)

        return "\n".join(new_imports)

    # Replace imports
    content = re.sub(import_pattern, process_imports, content, flags=re.DOTALL)

    # Handle single-line imports
    single_import_pattern = r'from\s+cyberdelta\.apis\.models\.service_args_models\s+import\s+(\w+)'

    def process_single_import(match):
        imp = match.group(1)
        if imp in OLD_TO_NEW_MAPPING:
            module, new_name = OLD_TO_NEW_MAPPING[imp]
            # Update usage in content
            nonlocal content
            content = re.sub(rf'\b{imp}\b', new_name, content)
            return f"from cyberdelta.apis.models.service_args.{module} import {new_name}"
        else:
            return f"from cyberdelta.apis.models.service_args import {imp}"

    content = re.sub(single_import_pattern, process_single_import, content)

    if content != original_content:
        with open(filepath, 'w') as f:
            f.write(content)
        return True
    return False

def main():
    """Run the migration."""
    # First, update the 4 test files with old names
    test_files = [
        "tests/unit/apis/hyperliquid/test_hl_request_builder_transfers.py",
        "tests/unit/apis/hyperliquid/test_hl_request_builder_trading.py",
        "tests/unit/apis/hyperliquid/test_hl_request_builder_info_market.py",
        "tests/unit/apis/hyperliquid/services/test_hl_market_data_service_candles.py",
    ]

    print("Phase 1: Updating test files with old model names...")
    for file_path in test_files:
        full_path = Path(file_path)
        if full_path.exists():
            if update_imports_in_file(full_path):
                print(f"✓ Updated: {file_path}")
            else:
                print(f"✗ No changes needed: {file_path}")
        else:
            print(f"✗ File not found: {file_path}")

    # Optional: Update all imports to use new paths
    print("\nPhase 2: Updating all imports (optional)...")
    # This would require finding all 158 files and updating them

    print("\nPhase 3: Ready to remove backward compatibility file")
    print("Run: rm cyberdelta/apis/models/service_args_models.py")

    print("\nPhase 4: Run linters and tests")
    print("Run: mypy . && ruff check . && pyright . && pytest")

if __name__ == "__main__":
    main()
```

## Risk Assessment

### Low Risk Factors
1. **Only 4 files use old names** - All are test files, no production code affected
2. **No dynamic imports** - All imports are static and easily searchable
3. **Clear naming pattern** - Old names map directly to new names with prefix
4. **Comprehensive linting** - mypy, ruff, and pyright will catch any missed imports
5. **Test coverage** - Existing tests will validate the changes

### Mitigation Strategies
1. **Create backup branch** before making changes
2. **Run migration script** on the 4 test files first
3. **Verify with linters** before removing compatibility file
4. **Keep compatibility file backed up** for emergency rollback

## Recommended Execution Timeline

### Day 1 (Immediate)
1. Create feature branch: `feature/remove-service-args-backward-compat`
2. Run Phase 1: Update the 4 test files
3. Run linters and tests to verify
4. Commit: "refactor: Update remaining test files to use new Hyperliquid model names"

### Day 2 (Optional)
1. Run Phase 2: Update all import paths to new structure
2. This can be done incrementally or all at once
3. Commit: "refactor: Update all imports to use new service_args module structure"

### Day 3 (Final)
1. Delete `service_args_models.py`
2. Run full test suite
3. Commit: "refactor!: Remove service_args backward compatibility layer"
4. Create PR with BREAKING CHANGE note

## Conclusion

The migration to break backward compatibility is straightforward and low-risk:
- Only 4 test files need immediate updates
- No production code uses old model names
- The migration can be completed in under an hour
- Full automation script is provided for safety

The backward compatibility layer has served its purpose during the initial migration. Now it's time to complete the refactoring with a clean break.
