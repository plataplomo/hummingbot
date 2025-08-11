# Event System Configuration Integration

## Overview
The event system retry logic is fully integrated with CyberDeltaEngine's configuration system through `@cyberdelta/config/`.

## Configuration Structure

### 1. YAML Configuration (`config.yaml`)
The event system configuration is defined in `config.yaml` under the `event_system` section:

```yaml
event_system:
  handler:
    max_consecutive_errors: 10
    auto_degrade_after_errors: 10
    auto_fault_after_errors: 20
    retry_config:
      max_attempts: 3
      initial_delay_sec: 1.0
      max_delay_sec: 60.0
      exponential_base: 2.0
      jitter: true
    cache_ttl_seconds: 300
    max_cache_size: 10000
    batch_size: 100
    processing_timeout_sec: 30.0
  
  workflow:
    workflow_timeout_sec: 300.0
    step_timeout_sec: 60.0
    retry_config:
      max_attempts: 3
      initial_delay_sec: 1.0
      max_delay_sec: 60.0
      exponential_base: 2.0
      jitter: true
```

### 2. Pydantic Models (`event_system_config.py`)
Configuration is validated through Pydantic models:
- `EventRetryConfig`: Retry configuration parameters
- `EventHandlerConfig`: Handler-specific configuration including retry
- `EventWorkflowConfig`: Workflow configuration including retry
- `EventSystemSettings`: Complete event system configuration

### 3. AppSettings Integration (`app_config.py`)
The event system configuration is integrated into the main `AppSettings` model:

```python
class AppSettings(BaseModel):
    event_system: EventSystemSettings = Field(
        default_factory=EventSystemSettings,
        description="Event system configuration for msgspec/bubus architecture"
    )
```

## Usage in Code

### Handler Creation
When creating event handlers, pass the configuration from AppSettings:

```python
from cyberdelta.config import get_app_settings

# Get configuration
app_settings = get_app_settings()
handler_config = app_settings.event_system.handler

# Create handler with configuration
handler = TradingOrderEventHandler(
    event_bus=event_bus,
    trading_service=trading_service,
    symbol_service=symbol_service,
    config=handler_config  # EventHandlerConfig from config.yaml
)
```

### Retry Utility
The `create_retryer()` helper uses the configuration:

```python
from cyberdelta.utils.retry_utils import create_retryer

# Create retryer with configuration
retryer = create_retryer(
    self.config.retry_config,  # EventRetryConfig from handler_config
    logger_name="my_handler"
)

# Execute with retry
result = await retryer(async_operation)
```

### Workflow Creation
Workflows also use configuration from AppSettings:

```python
workflow_config = app_settings.event_system.workflow

workflow = PlaceOrderWorkflow(
    event_bus=event_bus,
    risk_service=risk_service,
    trading_service=trading_service,
    config=workflow_config  # EventWorkflowConfig from config.yaml
)
```

## Configuration Values

### Handler Retry Configuration
- **max_attempts**: 3 - Maximum retry attempts for handler operations
- **initial_delay_sec**: 1.0 - Initial delay between retries
- **max_delay_sec**: 60.0 - Maximum delay between retries
- **exponential_base**: 2.0 - Base for exponential backoff
- **jitter**: true - Add randomization to retry delays

### Handler Error Handling
- **max_consecutive_errors**: 10 - Errors before considering degradation
- **auto_degrade_after_errors**: 10 - Auto-degrade after this many errors
- **auto_fault_after_errors**: 20 - Auto-fault after this many errors

### Workflow Retry Configuration
- **workflow_timeout_sec**: 300.0 - Overall workflow timeout
- **step_timeout_sec**: 60.0 - Individual step timeout
- Same retry parameters as handlers

## Zero Hardcoded Values

The implementation ensures ZERO hardcoded retry values:
1. All retry parameters come from configuration
2. No magic numbers in the code
3. Configuration can be adjusted without code changes
4. Different environments can have different retry behaviors

## Configuration Loading

Configuration is loaded and validated at startup:
1. `ConfigManager` loads `config.yaml`
2. Pydantic validates all values
3. Invalid configuration causes startup failure
4. Configuration is immutable after loading (frozen=True)

## Testing Configuration

To test configuration loading:
```python
from cyberdelta.config.config_manager import ConfigManager

config_manager = ConfigManager("cyberdelta/config/config.yaml")
settings = config_manager.settings

# Access event system configuration
event_config = settings.event_system
print(f"Handler max attempts: {event_config.handler.retry_config.max_attempts}")
print(f"Workflow timeout: {event_config.workflow.workflow_timeout_sec}")
```

## Benefits

1. **Centralized Configuration**: All retry settings in one place
2. **Type Safety**: Pydantic validation ensures correct types
3. **Environment Specific**: Different configs for dev/staging/prod
4. **No Code Changes**: Adjust retry behavior via config only
5. **Validation**: Invalid configuration fails fast at startup
6. **Documentation**: Configuration is self-documenting via Pydantic