# Any Type Reduction Plan

## Current Status
- Total Any occurrences: 317
- Target: <50
- Gap: Need to reduce by ~267 instances

## High-Impact Reductions (Quick Wins)

### 1. Replace to_dict() Return Types (~50 instances)
**Pattern**: `def to_dict(self) -> dict[str, Any]:`
**Solution**: Use TypedDict or specific return types

Example:
```python
# Before
def to_dict(self) -> dict[str, Any]:
    return {"name": self.name, "value": self.value}

# After
from typing import TypedDict

class ConfigDict(TypedDict):
    name: str
    value: float

def to_dict(self) -> ConfigDict:
    return {"name": self.name, "value": self.value}
```

### 2. Type Factory Functions (~30 instances)
**Pattern**: Functions with comments saying what they return
**Solution**: Use the type from the comment

Example:
```python
# Before
def _str_metrics_trend_dict_factory() -> dict[str, Any]:
    """Factory function that preserves dict[str, MetricsTrend] type."""
    return {}

# After  
def _str_metrics_trend_dict_factory() -> dict[str, MetricsTrend]:
    """Factory function that preserves dict[str, MetricsTrend] type."""
    return {}
```

### 3. Simple Metadata Types (~40 instances)
**Pattern**: `metadata: dict[str, Any]` for known simple types
**Solution**: `dict[str, str | int | float | bool]`

### 4. Stats/Metrics Methods (~20 instances)
**Pattern**: `get_stats() -> dict[str, Any]`
**Solution**: Create specific types

Example:
```python
# Before
def get_stats(self) -> dict[str, Any]:
    return {"count": 10, "rate": 0.95, "name": "test"}

# After
StatsDict = TypedDict('StatsDict', {
    'count': int,
    'rate': float, 
    'name': str
})

def get_stats(self) -> StatsDict:
    return {"count": 10, "rate": 0.95, "name": "test"}
```

## Implementation Priority

1. **Phase 1**: Factory functions (30 instances) - Easiest, just copy from comments
2. **Phase 2**: Simple metadata (40 instances) - Quick find/replace
3. **Phase 3**: to_dict() methods (50 instances) - Requires TypedDict definitions  
4. **Phase 4**: Stats/metrics (20 instances) - Requires understanding return structure

Total reduction: ~140 instances
New total: ~177 instances (still above target)

## Legitimate Any Usage to Keep

- Audit trail: `details: dict[str, Any]` (~10 instances)
- State snapshots: `state_data: dict[str, Any]` (~5 instances)
- Raw exchange data: `raw_data: dict[str, Any]` (~10 instances)
- Event handlers: Generic event handling (~20 instances)
- Validation callbacks: Async patterns (~15 instances)

Total legitimate: ~60-70 instances

## Success Criteria

After implementing the high-impact reductions, we should achieve:
- Total Any usage: <100 (from 317)
- All factory functions properly typed
- All simple metadata using union types
- Key serialization methods using TypedDict

This would be a significant improvement and get us much closer to the original <50 target.