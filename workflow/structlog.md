# Structlog Refactoring Workflow

## Overview

This document outlines the comprehensive workflow for migrating CyberDeltaEngine from mixed logging (standard Python logging + unconfigured structlog) to a fully structured logging system using structlog.

## Current State Analysis

### Logging Systems in Use
1. **Standard Python Logging**: 94 files using `logging.getLogger()`
2. **Structlog**: 8 files using `structlog.get_logger()` (unconfigured)
3. **Mixed Output**: File logging only captures standard logging, console shows both

### Issues with Current Setup
- Inconsistent log formats between modules
- Structlog messages not captured in log files
- No structured data capability for financial events
- Missing correlation IDs for tracking operations
- No consistent context propagation

## Migration Strategy

### Phase 1: Setup Structlog Configuration

#### 1.1 Create New Structlog Configuration Module
Create `cyberdelta/config/structlog_config.py`:

```python
"""Structured logging configuration for CyberDeltaEngine."""

from __future__ import annotations

import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog
from structlog.contextvars import merge_contextvars
from structlog.processors import CallsiteParameter, CallsiteParameterAdder
from structlog.typing import EventDict, Processor

from cyberdelta.config.config_models import AppSettings


def add_timestamp(_, __, event_dict: EventDict) -> EventDict:
    """Add ISO format timestamp to log events."""
    event_dict["timestamp"] = datetime.now(UTC).isoformat()
    return event_dict


def censor_sensitive_data(_, __, event_dict: EventDict) -> EventDict:
    """Remove or mask sensitive data from logs."""
    sensitive_keys = {
        "api_key", "secret", "password", "private_key",
        "seed_phrase", "auth_token", "signature"
    }

    for key in list(event_dict.keys()):
        if any(sensitive in key.lower() for sensitive in sensitive_keys):
            event_dict[key] = "***REDACTED***"

    return event_dict


def setup_structlog(app_settings: AppSettings) -> None:
    """Configure structlog for the application.

    Args:
        app_settings: Application configuration containing logging settings
    """
    # Determine log level
    log_level = getattr(logging, app_settings.general.log_level.upper(), logging.INFO)

    # Configure timestamping
    structlog.configure_once(
        processors=[
            # Add log level
            structlog.stdlib.add_log_level,
            # Add logger name
            structlog.stdlib.add_logger_name,
            # Add thread and process info
            structlog.processors.add_log_level,
            # Add timestamp
            add_timestamp,
            # Merge context variables
            merge_contextvars,
            # Add callsite parameters
            CallsiteParameterAdder(
                parameters=[
                    CallsiteParameter.FILENAME,
                    CallsiteParameter.LINENO,
                    CallsiteParameter.FUNC_NAME,
                ],
                additional_ignores=["structlog", "logging"],
            ),
            # Censor sensitive data
            censor_sensitive_data,
            # Format for development
            structlog.dev.ConsoleRenderer(colors=True)
            if app_settings.general.environment == "development"
            else structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Configure standard logging to work with structlog
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    # Setup file logging if configured
    if app_settings.general.log_file:
        setup_file_logging(app_settings.general.log_file, log_level)


def setup_file_logging(log_file: str, level: int) -> None:
    """Setup file logging handler.

    Args:
        log_file: Path to log file
        level: Logging level
    """
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Create file handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(level)

    # Use JSON formatter for file output
    formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
    )
    file_handler.setFormatter(formatter)

    # Add handler to root logger
    logging.getLogger().addHandler(file_handler)


def get_logger(name: str | None = None, **context: Any) -> structlog.BoundLogger:
    """Get a configured structlog logger.

    Args:
        name: Logger name (defaults to module name)
        **context: Additional context to bind to logger

    Returns:
        Configured structlog logger with context
    """
    logger = structlog.get_logger(name)
    if context:
        logger = logger.bind(**context)
    return logger
```

#### 1.2 Update Main Application Entry Point
Modify `main.py` to use new configuration:

```python
# Replace existing logging setup
from cyberdelta.config.structlog_config import setup_structlog, get_logger

# In main():
setup_structlog(app_settings)
logger = get_logger(__name__)
```

### Phase 2: Module Migration Plan

#### 2.1 Priority Order
Migrate modules in this order to minimize disruption:

1. **Core Financial Modules** (High Priority):
   - `risk_manager.py` - Critical for financial safety
   - `order_manager.py` - Order tracking needs structured data
   - `trade_executor.py` - Already uses structlog
   - `portfolio_tracker.py` - Position tracking benefits from structure

2. **Exchange APIs** (Medium Priority):
   - `hyperliquid/hl_api.py`
   - `backpack/bp_api.py`
   - Rate limiters and error handlers

3. **Supporting Modules** (Low Priority):
   - Utilities
   - Scripts
   - Test files

#### 2.2 Migration Pattern for Each Module

**Step 1: Update Import**
```python
# OLD
from cyberdelta.config.logging_config import get_logger
logger = get_logger(__name__)

# NEW
from cyberdelta.config.structlog_config import get_logger
logger = get_logger(__name__)
```

**Step 2: Enhance Logging with Structure**
```python
# OLD
logger.info(f"Order {order_id} filled at {price}")

# NEW
logger.info(
    "order_filled",
    order_id=order_id,
    price=price,
    exchange=exchange,
    symbol=symbol,
)
```

**Step 3: Add Context Where Appropriate**
```python
# For request handling
logger = logger.bind(request_id=request_id, user=user_address)

# For strategy operations
logger = logger.bind(strategy=strategy_name, symbol=symbol)
```

### Phase 3: Enhanced Logging Features

#### 3.1 Correlation IDs for Operations
```python
import uuid

def generate_operation_id() -> str:
    return str(uuid.uuid4())

# In trade execution
operation_id = generate_operation_id()
logger = logger.bind(operation_id=operation_id)
```

#### 3.2 Performance Metrics
```python
from contexttimer import Timer

with Timer() as timer:
    result = await execute_trade()

logger.info(
    "trade_executed",
    duration_ms=timer.elapsed * 1000,
    success=result.success,
)
```

#### 3.3 Structured Financial Events
```python
# Define structured event types using Pydantic
from pydantic import BaseModel, Field
from decimal import Decimal
from datetime import datetime

class OrderEvent(BaseModel):
    """Structured order event for logging."""
    event_type: str = Field(..., description="Type of order event")
    order_id: str = Field(..., description="Unique order identifier")
    symbol: str = Field(..., description="Trading symbol")
    exchange: str = Field(..., description="Exchange name")
    side: str = Field(..., description="Order side (BUY/SELL)")
    quantity: Decimal = Field(..., description="Order quantity")
    price: Decimal = Field(..., description="Order price")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))

    class Config:
        """Pydantic config for serialization."""
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat(),
        }

# Log structured events
order_event = OrderEvent(
    event_type="order_placed",
    order_id="12345",
    symbol="BTC-PERP",
    exchange="hyperliquid",
    side="BUY",
    quantity=Decimal("0.1"),
    price=Decimal("45000.50"),
)
logger.info("order_event", **order_event.model_dump())
```

### Phase 4: Testing Strategy

#### 4.1 Update Test Fixtures
```python
# In conftest.py
@pytest.fixture
def structured_log_capture():
    """Capture structured logs for testing."""
    from structlog.testing import LogCapture

    with LogCapture() as capture:
        yield capture
```

#### 4.2 Test Structured Logging
```python
def test_order_logging(structured_log_capture):
    # Execute operation
    order_manager.place_order(...)

    # Verify structured logs
    assert structured_log_capture.entries[0]["event"] == "order_placed"
    assert structured_log_capture.entries[0]["order_id"] == expected_id
```

### Phase 5: Rollout Plan

#### Week 1: Infrastructure Setup
- [ ] Implement `structlog_config.py`
- [ ] Update `main.py` to use new configuration
- [ ] Test file logging works correctly
- [ ] Deploy to dev environment

#### Week 2: Core Module Migration
- [ ] Migrate `risk_manager.py`
- [ ] Migrate `order_manager.py`
- [ ] Migrate `portfolio_tracker.py`
- [ ] Update `trade_executor.py` (already uses structlog)
- [ ] Test all core flows

#### Week 3: API Module Migration
- [ ] Migrate Hyperliquid API modules
- [ ] Migrate Backpack API modules
- [ ] Update rate limiters
- [ ] Test exchange integrations

#### Week 4: Cleanup and Optimization
- [ ] Migrate remaining modules
- [ ] Remove old logging configuration
- [ ] Update documentation
- [ ] Performance testing
- [ ] Production deployment

### Phase 6: Monitoring and Analysis

#### 6.1 Log Aggregation
Configure log aggregation to handle structured JSON logs:
- Set up log parsing for JSON format
- Create dashboards for key metrics
- Set up alerts based on structured fields

#### 6.2 Performance Impact
Monitor performance impact:
- Measure logging overhead
- Optimize processor chain if needed
- Consider async logging for high-frequency paths

## Best Practices

### Do's
- ✅ Always use structured fields instead of f-strings
- ✅ Bind context at appropriate levels (request, operation, strategy)
- ✅ Use consistent event names (snake_case)
- ✅ Include relevant business context
- ✅ Test logging output in unit tests

### Don'ts
- ❌ Don't log sensitive data (API keys, secrets)
- ❌ Don't use print statements
- ❌ Don't mix logging styles in same module
- ❌ Don't over-log in hot paths
- ❌ Don't forget to bind correlation IDs

## Configuration Examples

### Development Configuration
```python
structlog.configure(
    processors=[
        structlog.dev.ConsoleRenderer(colors=True),
    ],
)
```

### Production Configuration
```python
structlog.configure(
    processors=[
        structlog.processors.JSONRenderer(),
    ],
)
```

### Testing Configuration
```python
structlog.configure(
    processors=[
        structlog.testing.LogCapture(),
    ],
)
```

## Validation Checklist

Before considering migration complete:
- [ ] All modules use structlog
- [ ] File logging captures all events
- [ ] No mixed logging formats in output
- [ ] Correlation IDs implemented
- [ ] Sensitive data redaction working
- [ ] Performance metrics captured
- [ ] Tests updated for structured logging
- [ ] Documentation updated
- [ ] Monitoring dashboards configured
- [ ] Team trained on new logging patterns

## Troubleshooting

### Common Issues

1. **Missing logs in file**
   - Check file handler configuration
   - Verify log level settings
   - Ensure structlog processors include file output

2. **Performance degradation**
   - Review processor chain
   - Consider async logging
   - Profile hot paths

3. **Test failures**
   - Update test fixtures
   - Use LogCapture for assertions
   - Check for hardcoded log format expectations

## References

- [Structlog Documentation](https://www.structlog.org/)
- [Structlog Best Practices](https://www.structlog.org/en/stable/best-practices.html)
- [Python Logging HOWTO](https://docs.python.org/3/howto/logging.html)
