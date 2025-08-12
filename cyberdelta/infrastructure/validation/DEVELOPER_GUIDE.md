# Validation System Developer Guide

> **📚 Complete Guide to Extending the Unified Validation System**
>
> This guide explains how to add new validation rules, extend the validation framework, and maintain the validation system.

## Architecture Overview

### Core Components

```
UnifiedValidationService
├── ValidationRegistry
│   ├── ValidationRule (Protocol)
│   ├── ValidationCategory (Enum)
│   └── Rule Collections by Category
├── ValidationContext (Data)
└── ValidationResult (Output)
```

### Execution Flow

```
1. validate_order() called
2. ValidationContext built
3. Rules executed by category:
   ┌─────────────┬─────────────────────────────────────┐
   │ Category    │ Rules                               │
   ├─────────────┼─────────────────────────────────────┤
   │ PRECISION   │ PricePrecisionRule                  │
   │             │ QuantityPrecisionRule               │
   ├─────────────┼─────────────────────────────────────┤
   │ LIMITS      │ OrderValueLimitsRule                │
   ├─────────────┼─────────────────────────────────────┤
   │ BALANCE     │ BalanceValidationRule               │
   ├─────────────┼─────────────────────────────────────┤
   │ RISK        │ MaxPositionRule                     │
   │             │ MaxExposureRule                     │
   ├─────────────┼─────────────────────────────────────┤
   │ MARKET      │ MarketStatusRule                    │
   │             │ TradingHoursRule                    │
   │             │ LiquidityRule                       │
   ├─────────────┼─────────────────────────────────────┤
   │ STATE       │ (Future expansion)                  │
   └─────────────┴─────────────────────────────────────┘
4. Results aggregated
5. ValidationResult returned
```

## Creating New Validation Rules

### Step 1: Implement ValidationRule Protocol

```python
from cyberdelta.protocols.validation import ValidationRule
from cyberdelta.enums import ValidationCategory
from cyberdelta.models.validation import ValidationResult

class MyCustomRule:
    """Custom validation rule for [describe purpose]."""
    
    def __init__(self, enabled: bool = True) -> None:
        """Initialize the rule.
        
        Args:
            enabled: Whether this rule is enabled
        """
        self._enabled = enabled
    
    @property
    def name(self) -> str:
        """Rule name for identification."""
        return "my_custom_rule"
    
    @property 
    def category(self) -> ValidationCategory:
        """Rule category determines execution order."""
        return ValidationCategory.LIMITS  # Choose appropriate category
    
    @property
    def enabled(self) -> bool:
        """Whether rule is enabled."""
        return self._enabled
    
    @property
    def bypass_on_reduce_only(self) -> bool:
        """Whether to skip this rule for reduce-only orders."""
        return False  # Set True if rule should be bypassed for position closing
    
    async def validate(
        self, 
        order: Order, 
        context: ValidationContext
    ) -> ValidationResult:
        """Validate order against this rule.
        
        Args:
            order: Order to validate
            context: Validation context with configuration and market data
            
        Returns:
            ValidationResult with violations if any
        """
        violations: list[str] = []
        
        # Implement your validation logic here
        if self._should_validate(order, context):
            violation = self._check_custom_condition(order, context)
            if violation:
                violations.append(violation)
        
        return ValidationResult(
            violations=violations,
            category=self.category,
            rule_name=self.name,
        )
    
    def _should_validate(self, order: Order, context: ValidationContext) -> bool:
        """Check if this rule should run for the given order/context."""
        # Example: Only validate for certain exchanges
        return order.exchange in [ExchangeName.BACKPACK, ExchangeName.HYPERLIQUID]
    
    def _check_custom_condition(
        self, 
        order: Order, 
        context: ValidationContext
    ) -> str | None:
        """Implement your custom validation logic.
        
        Returns:
            Violation message if validation fails, None if passes
        """
        # Example validation
        if context.has_exchange_config():
            exchange_config = context.exchange_config
            custom_limit = getattr(exchange_config, 'my_custom_limit', None)
            
            if custom_limit and order.quantity_requested > custom_limit:
                return (
                    f"Order quantity {order.quantity_requested} exceeds "
                    f"custom limit {custom_limit} for {order.symbol.value}"
                )
        
        return None
```

### Step 2: Register the Rule

Add your rule to the `UnifiedValidationService`:

```python
# In cyberdelta/domain/trading/validation/unified_validation_service.py

def _register_validation_rules(self) -> None:
    """Register all validation rules with the registry."""
    
    # Existing rules...
    self.registry.register(PricePrecisionRule(enabled=True))
    # ... other existing rules
    
    # Add your custom rule
    self.registry.register(MyCustomRule(enabled=True))
```

### Step 3: Add Configuration Support

Add configuration options for your rule:

```python
# In your rule's validate method
def _get_custom_config_value(self, context: ValidationContext) -> Decimal | None:
    """Get custom configuration value."""
    
    # From exchange-specific config
    if context.has_exchange_config() and context.exchange_config:
        return getattr(context.exchange_config, 'my_custom_setting', None)
    
    # From global validation config
    validation_config = getattr(context.config, 'validation', None)
    if validation_config:
        return getattr(validation_config, 'my_custom_setting', None)
    
    return None
```

### Step 4: Write Tests

Create comprehensive tests for your rule:

```python
# tests/unit/domain/trading/validation/rules/test_my_custom_rule.py

import pytest
from decimal import Decimal
from unittest.mock import Mock

from cyberdelta.domain.trading.validation.rules.my_custom_rule import MyCustomRule
from cyberdelta.domain.trading.validation.validation_context import ValidationContext
from cyberdelta.enums import ValidationCategory
from cyberdelta.models.market.order import Order


class TestMyCustomRule:
    """Test suite for MyCustomRule."""
    
    @pytest.fixture
    def rule(self) -> MyCustomRule:
        """Create rule instance for testing."""
        return MyCustomRule(enabled=True)
    
    @pytest.fixture
    def mock_context(self) -> Mock:
        """Create mock validation context."""
        context = Mock(spec=ValidationContext)
        # Set up context mocks as needed
        return context
    
    @pytest.fixture  
    def test_order(self) -> Order:
        """Create test order."""
        return Order(
            exchange=ExchangeName.BACKPACK,
            symbol=bp_symbol("BTC-USDC"),
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity_requested=Decimal("1.0"),
            time_in_force=TimeInForce.GTC,
            # ... other required fields
        )
    
    def test_rule_properties(self, rule: MyCustomRule) -> None:
        """Test rule basic properties."""
        assert rule.name == "my_custom_rule"
        assert rule.category == ValidationCategory.LIMITS
        assert rule.enabled is True
        assert rule.bypass_on_reduce_only is False
    
    @pytest.mark.asyncio
    async def test_validation_passes_when_condition_met(
        self,
        rule: MyCustomRule,
        test_order: Order,
        mock_context: Mock
    ) -> None:
        """Test validation passes when conditions are met."""
        # Set up context for passing condition
        mock_context.has_exchange_config.return_value = True
        mock_context.exchange_config.my_custom_limit = Decimal("10.0")
        
        result = await rule.validate(test_order, mock_context)
        
        assert result.violations == []
        assert result.category == ValidationCategory.LIMITS
        assert result.rule_name == "my_custom_rule"
    
    @pytest.mark.asyncio
    async def test_validation_fails_when_condition_not_met(
        self,
        rule: MyCustomRule,
        test_order: Order,
        mock_context: Mock
    ) -> None:
        """Test validation fails when conditions are not met."""
        # Set up context for failing condition
        mock_context.has_exchange_config.return_value = True
        mock_context.exchange_config.my_custom_limit = Decimal("0.5")
        
        result = await rule.validate(test_order, mock_context)
        
        assert len(result.violations) == 1
        assert "exceeds custom limit" in result.violations[0]
        assert "BTC-USDC" in result.violations[0]
    
    @pytest.mark.asyncio
    async def test_disabled_rule_skipped(
        self,
        mock_context: Mock,
        test_order: Order
    ) -> None:
        """Test that disabled rules are skipped."""
        disabled_rule = MyCustomRule(enabled=False)
        
        # Rule should be skipped at registry level
        assert disabled_rule.enabled is False
```

## Validation Categories

### Choosing the Right Category

Categories determine execution order and fail-fast behavior:

```python
class ValidationCategory(Enum):
    """Validation categories in execution order."""
    
    PRECISION = "precision"    # Tick/lot size alignment (fail-fast)
    LIMITS = "limits"         # Min/max value constraints  
    BALANCE = "balance"       # Fund availability checks
    RISK = "risk"            # Position/exposure limits
    MARKET = "market"        # Market conditions/liquidity
    STATE = "state"          # System state checks
```

### Category Guidelines

- **PRECISION**: Use for fundamental order format validation (prices, quantities)
- **LIMITS**: Use for basic business rule constraints (min/max values)
- **BALANCE**: Use for account balance and fund availability checks
- **RISK**: Use for position sizing and risk management rules
- **MARKET**: Use for market condition and liquidity validation  
- **STATE**: Use for system state and timing validation

### Fail-Fast Behavior

PRECISION category rules trigger fail-fast behavior:

```python
# In UnifiedValidationService
if category == ValidationCategory.PRECISION and category_result.violations:
    logger.warning("Stopping validation due to critical precision errors")
    break  # No further categories executed
```

## Advanced Features

### Configuration-Driven Rules

Make rules configurable via AppSettings:

```python
class ConfigurableRule:
    """Rule with configuration support."""
    
    def __init__(self, config: AppSettings) -> None:
        self._config = config
        
        # Extract rule-specific config
        rule_config = getattr(config.validation, 'my_rule', None)
        self._enabled = getattr(rule_config, 'enabled', True)
        self._threshold = getattr(rule_config, 'threshold', Decimal("1000"))
        self._exchanges = getattr(rule_config, 'exchanges', [])
    
    @property
    def enabled(self) -> bool:
        return self._enabled
```

### Context-Aware Validation

Use validation context effectively:

```python
async def validate(self, order: Order, context: ValidationContext) -> ValidationResult:
    """Context-aware validation."""
    violations = []
    
    # Check trading state
    if context.trading_state == TradingState.HALTED:
        if not context.is_reduce_only:
            violations.append("Trading halted - only reduce-only orders allowed")
    
    # Use market data
    if context.has_market_data() and context.market_snapshot:
        market_price = context.market_snapshot.price
        if abs(order.price - market_price) / market_price > Decimal("0.05"):
            violations.append(f"Order price {order.price} too far from market {market_price}")
    
    # Use portfolio data
    if context.portfolio_state:
        current_position = context.portfolio_state.get_position(order.symbol)
        # Validate against current position
    
    return ValidationResult(violations=violations)
```

### Bypass Logic for Reduce-Only Orders

Implement smart bypass logic:

```python
@property
def bypass_on_reduce_only(self) -> bool:
    """Allow position closing even during restricted conditions."""
    return True  # This rule allows reduce-only bypass

async def validate(self, order: Order, context: ValidationContext) -> ValidationResult:
    """Validate with reduce-only awareness."""
    
    # Rule logic that might be restrictive
    if some_restrictive_condition:
        if context.is_reduce_only and self.bypass_on_reduce_only:
            logger.info(f"Bypassing {self.name} for reduce-only order")
            return ValidationResult(violations=[])
        else:
            return ValidationResult(violations=["Restrictive condition failed"])
    
    return ValidationResult(violations=[])
```

### Error Handling Best Practices

Handle errors gracefully in rules:

```python
async def validate(self, order: Order, context: ValidationContext) -> ValidationResult:
    """Validate with proper error handling."""
    violations = []
    
    try:
        # Your validation logic
        result = self._perform_validation(order, context)
        if not result:
            violations.append("Validation failed")
            
    except Exception as e:
        # Log the error but don't crash validation
        logger.exception(
            "validation_rule_error",
            rule_name=self.name,
            order_id=getattr(order, 'exchange_order_id', 'unknown'),
            error=str(e)
        )
        # Treat as validation failure
        violations.append(f"Rule {self.name} failed due to error: {e}")
    
    return ValidationResult(violations=violations)
```

## Testing Best Practices

### Test Structure

```python
class TestMyRule:
    """Follow this test structure."""
    
    # 1. Fixtures for rule, orders, context
    @pytest.fixture
    def rule(self) -> MyRule:
        pass
    
    # 2. Test basic properties
    def test_rule_properties(self) -> None:
        pass
    
    # 3. Test passing conditions  
    @pytest.mark.asyncio
    async def test_validation_passes_when_valid(self) -> None:
        pass
    
    # 4. Test failing conditions
    @pytest.mark.asyncio
    async def test_validation_fails_when_invalid(self) -> None:
        pass
    
    # 5. Test edge cases
    @pytest.mark.asyncio
    async def test_edge_cases(self) -> None:
        pass
    
    # 6. Test configuration variations
    @pytest.mark.asyncio
    async def test_different_configs(self) -> None:
        pass
    
    # 7. Test error handling
    @pytest.mark.asyncio
    async def test_error_handling(self) -> None:
        pass
```

### Property-Based Testing

Use Hypothesis for comprehensive testing:

```python
from hypothesis import given, strategies as st

class TestMyRuleProperties:
    """Property-based tests."""
    
    @given(
        quantity=st.decimals(min_value=0, max_value=1000000, places=8),
        price=st.decimals(min_value=0, max_value=100000, places=8)
    )
    @pytest.mark.asyncio
    async def test_rule_with_random_values(
        self,
        quantity: Decimal,
        price: Decimal
    ) -> None:
        """Test rule behavior with random values."""
        # Property: Rule should never crash with valid input
        rule = MyRule()
        order = create_order(quantity=quantity, price=price)
        context = create_context()
        
        result = await rule.validate(order, context)
        
        # Rule should always return ValidationResult
        assert isinstance(result, ValidationResult)
        assert isinstance(result.violations, list)
```

## Performance Considerations

### Efficient Validation

```python
async def validate(self, order: Order, context: ValidationContext) -> ValidationResult:
    """Efficient validation implementation."""
    
    # Early exit for disabled rule
    if not self.enabled:
        return ValidationResult(violations=[])
    
    # Quick checks first
    if not self._should_validate(order, context):
        return ValidationResult(violations=[])
    
    # Expensive operations only if needed
    if self._needs_expensive_check(order, context):
        result = await self._expensive_validation(order, context)
        return result
    
    return ValidationResult(violations=[])

def _needs_expensive_check(self, order: Order, context: ValidationContext) -> bool:
    """Check if expensive validation is needed."""
    # Use simple conditions to avoid expensive operations
    return order.quantity_requested > self._threshold
```

### Caching Strategies

```python
from functools import lru_cache

class OptimizedRule:
    """Rule with caching for expensive operations."""
    
    @lru_cache(maxsize=128)
    def _get_cached_config(self, exchange: str) -> dict:
        """Cache expensive config lookups."""
        return self._expensive_config_lookup(exchange)
    
    async def validate(self, order: Order, context: ValidationContext) -> ValidationResult:
        """Use cached config."""
        config = self._get_cached_config(order.exchange.value)
        # Use cached config for validation
```

## Common Patterns

### Multi-Condition Rules

```python
async def validate(self, order: Order, context: ValidationContext) -> ValidationResult:
    """Rule with multiple validation conditions."""
    violations = []
    
    # Condition 1: Basic validation
    basic_violation = self._check_basic_condition(order, context)
    if basic_violation:
        violations.append(basic_violation)
    
    # Condition 2: Advanced validation (only if basic passes)
    if not violations:
        advanced_violation = self._check_advanced_condition(order, context)
        if advanced_violation:
            violations.append(advanced_violation)
    
    return ValidationResult(violations=violations)
```

### Exchange-Specific Rules

```python
class ExchangeSpecificRule:
    """Rule that varies by exchange."""
    
    def __init__(self, enabled: bool = True) -> None:
        self._enabled = enabled
        self._exchange_handlers = {
            ExchangeName.BACKPACK: self._validate_backpack,
            ExchangeName.HYPERLIQUID: self._validate_hyperliquid,
        }
    
    async def validate(self, order: Order, context: ValidationContext) -> ValidationResult:
        """Exchange-specific validation."""
        handler = self._exchange_handlers.get(order.exchange)
        if not handler:
            return ValidationResult(violations=[
                f"Unsupported exchange: {order.exchange.value}"
            ])
        
        return await handler(order, context)
    
    async def _validate_backpack(
        self, 
        order: Order, 
        context: ValidationContext
    ) -> ValidationResult:
        """Backpack-specific validation."""
        # Implement Backpack-specific logic
        pass
    
    async def _validate_hyperliquid(
        self, 
        order: Order, 
        context: ValidationContext
    ) -> ValidationResult:
        """Hyperliquid-specific validation.""" 
        # Implement Hyperliquid-specific logic
        pass
```

## Future Extensions

### Plugin Architecture (Future)

```python
# Future: Dynamic rule loading
class PluginRule(ValidationRule):
    """Base class for plugin-based rules."""
    
    @classmethod
    def from_config(cls, config: dict) -> 'PluginRule':
        """Create rule from configuration."""
        pass
    
    def to_config(self) -> dict:
        """Serialize rule to configuration."""
        pass

# Future: Runtime rule management
class RuleManager:
    """Manage rules at runtime."""
    
    async def add_rule(self, rule: ValidationRule) -> None:
        """Add rule to active validation."""
        pass
    
    async def remove_rule(self, rule_name: str) -> None:
        """Remove rule from active validation."""
        pass
    
    async def update_rule_config(self, rule_name: str, config: dict) -> None:
        """Update rule configuration."""
        pass
```

### A/B Testing Framework (Future)

```python
# Future: A/B testing for validation rules
class ABTestRule(ValidationRule):
    """Rule that supports A/B testing."""
    
    def __init__(self, variant_a: ValidationRule, variant_b: ValidationRule):
        self._variant_a = variant_a
        self._variant_b = variant_b
    
    async def validate(self, order: Order, context: ValidationContext) -> ValidationResult:
        """Run A/B test validation."""
        # Determine which variant to use
        variant = self._select_variant(order, context)
        return await variant.validate(order, context)
```

## Summary

The unified validation system provides a powerful, extensible framework for order validation. Key principles:

1. **Implement ValidationRule protocol** for consistent interfaces
2. **Choose appropriate category** for execution order
3. **Handle errors gracefully** to maintain system stability  
4. **Write comprehensive tests** including property-based testing
5. **Consider performance** in rule implementation
6. **Use configuration** for flexibility
7. **Follow established patterns** for consistency

This architecture enables easy extension while maintaining type safety, performance, and reliability.