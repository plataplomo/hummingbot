# PositionManager `| None` Refactoring Plan

## Objective
Remove all `| None` usage from PositionManager to comply with CODING_STANDARDS.md strict no-fallback policy.

## Current State
- 8 instances of `| None` usage
- Mix of fallbacks, sentinels, and multi-purpose methods
- Violates project's zero-tolerance for assumptions and fallbacks

## Target State
- Zero `| None` usage
- Explicit methods for each operation
- Proper exception handling
- Type-safe result objects

## Refactoring Tasks

### Task 1: Create Required Types
```python
# cyberdelta/exceptions/portfolio.py
class PositionNotFoundError(PortfolioError):
    """Raised when requested position doesn't exist."""
    pass

# cyberdelta/models/portfolio/fill_result.py
@dataclass(frozen=True)
class FillApplicationResult:
    """Result of applying a fill to a position."""
    realized_pnl: Decimal
    new_average_price: Decimal
    was_reducing_position: bool
    position_closed: bool

# cyberdelta/models/portfolio/position_change.py
@dataclass(frozen=True)
class PositionChangeResult:
    """Result of calculating position change from fill."""
    new_quantity: Decimal
    realized_pnl: Decimal
    was_reducing: bool
```

### Task 2: Refactor Dependency Injection
**File**: `cyberdelta/domain/portfolio/position_manager.py`

```python
# Line 46-66: Remove optional calculator
def __init__(
    self,
    config: AppSettings,
    state_manager: PortfolioStateManagerProtocol,
    pnl_calculator: PnLCalculatorProtocol,  # Now required
) -> None:
    """Initialize position manager.
    
    Args:
        config: Application settings
        state_manager: Portfolio state manager
        pnl_calculator: PnL calculator for centralized calculations (required)
    """
    self.config = config
    self._state_manager = state_manager
    self._pnl_calculator = pnl_calculator  # No fallback
```

### Task 3: Replace None Returns with Exceptions
**File**: `cyberdelta/domain/portfolio/position_manager.py`

```python
# Line 73-92: Throw exception for missing position
async def get_position(
    self,
    symbol: Symbol,
    exchange: ExchangeName,
) -> DerivativePosition:  # No longer returns None
    """Get position for specific symbol on exchange.
    
    Raises:
        PositionNotFoundError: If position doesn't exist
    """
    state = await self._state_manager.get_state()
    if not state:
        raise PortfolioNotInitializedError()
    
    key = f"{exchange.value}:{symbol.value}"
    position = state.positions.get(key)
    if position is None:
        raise PositionNotFoundError(
            f"No position found for {symbol.value} on {exchange.value}"
        )
    return position

# Add new method for checking existence
async def has_position(
    self,
    symbol: Symbol,
    exchange: ExchangeName,
) -> bool:
    """Check if position exists without throwing exception."""
    state = await self._state_manager.get_state()
    if not state:
        return False
    
    key = f"{exchange.value}:{symbol.value}"
    return key in state.positions
```

### Task 4: Split Multi-Purpose Methods
**File**: `cyberdelta/domain/portfolio/position_manager.py`

```python
# Lines 94-116: Split get_all_positions
async def get_all_positions(self) -> dict[str, DerivativePosition]:
    """Get all positions across all exchanges."""
    state = await self._state_manager.get_state()
    if not state:
        return {}
    return state.positions.copy()

async def get_positions_for_exchange(
    self,
    exchange: ExchangeName
) -> dict[str, DerivativePosition]:
    """Get all positions for specific exchange."""
    state = await self._state_manager.get_state()
    if not state:
        return {}
    
    prefix = f"{exchange.value}:"
    return {
        k: v for k, v in state.positions.items() 
        if k.startswith(prefix)
    }

# Lines 221-237: Split get_total_exposure
async def get_total_exposure(self) -> Decimal:
    """Get total position exposure across all exchanges."""
    positions = await self.get_all_positions()
    return self._calculate_exposure(positions)

async def get_exchange_exposure(
    self,
    exchange: ExchangeName
) -> Decimal:
    """Get total position exposure for specific exchange."""
    positions = await self.get_positions_for_exchange(exchange)
    return self._calculate_exposure(positions)

def _calculate_exposure(
    self,
    positions: dict[str, DerivativePosition]
) -> Decimal:
    """Calculate total exposure for given positions."""
    total = Decimal(0)
    for position in positions.values():
        if position.entry_price:
            total += position.size * position.entry_price
    return total
```

### Task 5: Fix PnL Return Types
**File**: `cyberdelta/domain/portfolio/position_manager.py`

```python
# Line 114-181: Always return Decimal for PnL
async def update_position_from_fill(self, fill: Fill) -> Decimal:
    """Update position based on fill execution.
    
    Returns:
        Realized PnL (Decimal(0) if position opened/increased)
    """
    # ... existing logic ...
    
    if position:
        result = self._apply_fill_to_position(position, fill)
        realized_pnl = result.realized_pnl  # Always Decimal
        new_avg_price = result.new_average_price
    else:
        realized_pnl = Decimal(0)  # Not None
        new_avg_price = fill.price
    
    # ... rest of logic ...
    return realized_pnl  # Always Decimal

# Lines 406-446: Use result object
def _apply_fill_to_position(
    self,
    position: DerivativePosition,
    fill: Fill
) -> FillApplicationResult:  # New return type
    """Apply fill to position and calculate realized PnL."""
    new_quantity = self._calculate_new_quantity(position, fill)
    
    # Calculate PnL if reducing
    current_qty = position.size if position.side == OrderSide.BUY else -position.size
    is_reducing = current_qty != 0 and abs(new_quantity) < abs(current_qty)
    
    if is_reducing:
        pnl_result = self._pnl_calculator.calculate_realized_pnl(
            position=position,
            fill=fill,
            include_fees=False
        )
        realized_pnl = pnl_result.amount
    else:
        realized_pnl = Decimal(0)  # Not None
    
    new_average_price = self._calculate_average_price(position, fill, new_quantity)
    
    return FillApplicationResult(
        realized_pnl=realized_pnl,
        new_average_price=new_average_price,
        was_reducing_position=is_reducing,
        position_closed=abs(new_quantity) < self._position_closure_threshold
    )
```

### Task 6: Separate Update and Delete Operations
**File**: `cyberdelta/domain/portfolio/position_manager.py`

```python
# Line 366-404: Split into two methods
async def update_position_directly(
    self,
    symbol: Symbol,
    exchange: ExchangeName,
    new_position: DerivativePosition  # No longer optional
) -> None:
    """Update position with new data."""
    state = await self._state_manager.get_state()
    if not state:
        raise PortfolioNotInitializedError()
    
    key = f"{exchange.value}:{symbol.value}"
    state.positions[key] = new_position
    
    logger.info(
        "position_updated_directly",
        exchange=exchange.value,
        symbol=symbol.value,
        side=new_position.side.value,
        size=new_position.size,
        entry_price=new_position.entry_price
    )
    
    state.timestamp = datetime.now(UTC)
    await self._state_manager.save_state()

async def remove_position_directly(
    self,
    symbol: Symbol,
    exchange: ExchangeName
) -> None:
    """Remove position from state."""
    state = await self._state_manager.get_state()
    if not state:
        raise PortfolioNotInitializedError()
    
    key = f"{exchange.value}:{symbol.value}"
    if key not in state.positions:
        raise PositionNotFoundError(
            f"Cannot remove non-existent position: {symbol.value} on {exchange.value}"
        )
    
    del state.positions[key]
    
    logger.info(
        "position_removed_directly",
        exchange=exchange.value,
        symbol=symbol.value
    )
    
    state.timestamp = datetime.now(UTC)
    await self._state_manager.save_state()
```

## Update Call Sites

### Update Tests
```python
# Before
position = await manager.get_position(symbol, exchange)
if position:
    # work with position

# After
try:
    position = await manager.get_position(symbol, exchange)
    # work with position
except PositionNotFoundError:
    # handle missing position

# Or use new method
if await manager.has_position(symbol, exchange):
    position = await manager.get_position(symbol, exchange)
```

### Update Service Consumers
```python
# Before
all_positions = await manager.get_all_positions(exchange=ExchangeName.HYPERLIQUID)

# After
hl_positions = await manager.get_positions_for_exchange(ExchangeName.HYPERLIQUID)

# Before
exposure = await manager.get_total_exposure(exchange=exchange)

# After
if exchange:
    exposure = await manager.get_exchange_exposure(exchange)
else:
    exposure = await manager.get_total_exposure()
```

## Testing Strategy

### New Test Cases
1. Test that `PositionNotFoundError` is raised for missing positions
2. Test that PnL methods always return Decimal
3. Test new split methods work correctly
4. Test that constructor requires calculator
5. Test new result types contain correct data

### Update Existing Tests
1. Remove tests expecting None returns
2. Update tests to handle exceptions
3. Update tests to use new split methods
4. Update mock setups to provide calculator

## Rollout Plan

### Phase 1: Add New Code (Day 1-2)
- Create new exception and result types
- Add new methods alongside existing ones
- Mark old methods as deprecated with warnings

### Phase 2: Update Consumers (Day 3-4)
- Update all internal code to use new methods
- Update tests to use new patterns
- Run full test suite

### Phase 3: Remove Old Code (Day 5)
- Remove deprecated methods
- Remove all `| None` type hints
- Final test run and validation

## Success Metrics
- Zero `| None` usage in PositionManager
- All tests passing
- No runtime None-related errors
- Code review approval confirming CODING_STANDARDS.md compliance

## Risk Mitigation
- Keep old methods during transition with deprecation warnings
- Extensive testing at each phase
- Code review at each phase
- Rollback plan if issues discovered