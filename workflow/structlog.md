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
**Status: Completed ✓**

#### 1.1 Create New Structlog Configuration Module
Created `cyberdelta/config/structlog_config.py`:

```python
"""Structured logging configuration for CyberDeltaEngine."""

from __future__ import annotations

import logging
import re
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


def strip_ansi_codes(_, __, event_dict: EventDict) -> EventDict:
    """Strip ANSI color codes from all string values in event dict."""
    ansi_pattern = re.compile(r'\x1b\[[0-9;]*m')

    def strip_value(value: Any) -> Any:
        if isinstance(value, str):
            return ansi_pattern.sub('', value)
        elif isinstance(value, dict):
            return {k: strip_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [strip_value(v) for v in value]
        else:
            return value

    return {key: strip_value(value) for key, value in event_dict.items()}


def setup_structlog(app_settings: AppSettings) -> None:
    """Configure structlog for the application.

    Args:
        app_settings: Application configuration containing logging settings
    """
    # Determine log level
    log_level = getattr(logging, app_settings.general.log_level.upper(), logging.INFO)

    # Configure standard logging first
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
        force=True,  # Force reconfiguration
    )

    # Setup file logging if configured
    if app_settings.general.log_file:
        setup_file_logging(app_settings.general.log_file, log_level)

    # Base processors
    base_processors = [
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
        # Process positional arguments
        structlog.stdlib.PositionalArgumentsFormatter(),
        # Process stack info
        structlog.processors.StackInfoRenderer(),
        # Process exceptions
        structlog.processors.format_exc_info,
        # Add log level and logger name
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
    ]

    # Configure structlog for console output only
    # File output is handled by stdlib logging handlers with ProcessorFormatter
    structlog.configure(
        processors=base_processors + [
            # Render as colored console output
            structlog.dev.ConsoleRenderer(colors=True),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def setup_file_logging(log_file: str, level: int) -> None:
    """Setup file logging handler with JSON output.

    Args:
        log_file: Path to log file
        level: Logging level
    """
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Remove any existing file handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        if isinstance(handler, logging.FileHandler):
            root_logger.removeHandler(handler)

    # Create file handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(level)

    # Create processor formatter for clean JSON output
    # Use separate processors that strip ANSI codes before JSON rendering
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            # Extract from structlog's context and remove meta
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            # Strip ANSI codes before JSON rendering
            strip_ansi_codes,
            # Render as clean JSON
            structlog.processors.JSONRenderer(),
        ],
        foreign_pre_chain=[
            # For non-structlog logs (shouldn't happen but just in case)
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            add_timestamp,
            censor_sensitive_data,
            strip_ansi_codes,
        ],
    )

    file_handler.setFormatter(formatter)

    # Add to root logger
    root_logger.addHandler(file_handler)


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
Updated `main.py` to use new configuration:

```python
# Replace existing logging setup
from cyberdelta.config.structlog_config import setup_structlog, get_logger

# In main():
setup_structlog(app_settings)
logger = get_logger(__name__)
```

#### 1.3 Create Logging Helpers
Created `cyberdelta/logging/logging_helpers.py` with shared helpers for structured logging of financial events using existing Pydantic models.

#### 1.4 Key Features Implemented
- ✅ Dual output: Colored console + Clean JSON file logging
- ✅ ANSI color stripping for file output
- ✅ Sensitive data redaction
- ✅ Context propagation with contextvars
- ✅ Callsite information (filename, line number, function)
- ✅ Timestamp in ISO format
- ✅ Integration with existing Pydantic models

### Phase 2: Module Migration Plan
**Status: Ready to Begin**

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

**Step 2: Enhance Logging with Structure - CRITICAL PRINCIPLES**

⚠️ **IMPORTANT: Message Quality Must Be Maintained or Improved!**

When converting f-string logs to structured logs:
1. The event name should be descriptive and actionable
2. Include ALL information from the original message as structured fields
3. Consider adding a `message` field for complex human-readable context
4. Never lose information during migration

```python
# OLD - Good, informative message
logger.info(
    f"Order {order_id} on {exchange_id} is {order.status}. "
    f"Triggering trade processing (placeholder)."
)

# BAD MIGRATION - Lost critical context! ❌
log_order_lifecycle(logger, order, "filled")

# GOOD MIGRATION - Preserves all information ✅
logger.info(
    "order_fill_triggering_trade_processing",
    order_id=order_id,
    exchange_id=exchange_id,
    status=order.status.value,
    action="triggering_trade_processing",
    placeholder=True,
    # Optional: Include original message for backwards compatibility
    message=f"Order {order_id} on {exchange_id} is {order.status}. Triggering trade processing (placeholder)."
)
```

**Step 3: Add Context Where Appropriate**
```python
# For request handling
logger = logger.bind(request_id=request_id, user=user_address)

# For strategy operations
logger = logger.bind(strategy=strategy_name, symbol=symbol)
```

**Step 4: Validation After Migration**
- Compare old vs new log output side-by-side
- Ensure no information is lost
- Verify that someone reading logs can understand what happened
- Test that log queries/filtering still work effectively

### Phase 2.5: Message Quality Guidelines

#### Maintaining Informativeness

The goal of structured logging is to make logs more queryable AND maintain readability. When migrating:

1. **Event Names Should Tell a Story**
   ```python
   # Bad: Too generic
   "order_update"

   # Good: Specific and actionable
   "order_fill_detected_triggering_trade_processing"
   "order_rejected_insufficient_balance"
   "order_placement_failed_rate_limit"
   ```

2. **Include Human-Readable Context When Needed**
   ```python
   # For complex workflows, include a message field
   logger.info(
       "complex_arbitrage_decision",
       opportunity_id=opp.id,
       funding_rate_delta=float(opp.funding_delta),
       decision="rejected",
       reason="insufficient_liquidity",
       message=f"Arbitrage opportunity {opp.id} rejected: "
               f"Funding delta {opp.funding_delta:.4f} meets threshold but "
               f"liquidity {opp.liquidity:.2f} below minimum {min_liquidity:.2f}"
   )
   ```

3. **Preserve Original Intent**
   - If the original log explained WHY something happened, include that
   - If it showed a sequence of events, maintain that narrative
   - If it included calculations or comparisons, keep those

4. **Test Readability**
   - After migration, read the logs in both console and JSON format
   - Ensure a new team member could understand what happened
   - Verify that debugging remains effective

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

Create shared logging helpers in `cyberdelta/logging/logging_helpers.py`:

```python
"""Shared helpers for structured logging of financial events."""

from __future__ import annotations

from typing import Any, Type

import structlog
from pydantic import BaseModel

from cyberdelta.core.models.derivative_position import DerivativePosition
from cyberdelta.core.models.margin_account import MarginAccountSummary
from cyberdelta.core.models.market.order import Order
from cyberdelta.core.models.market.trade import Trade
from cyberdelta.core.models.trade_signal import TradeSignal

# Define sensitive fields to exclude per model type
SENSITIVE_FIELDS: dict[Type[BaseModel], set[str]] = {
    Order: {"trades", "hl_details", "bp_details"},
    Trade: {"hl_details", "bp_details"},
    TradeSignal: {"metadata"},  # May contain strategy-specific sensitive data
    DerivativePosition: {"hl_details", "bp_details"},
    MarginAccountSummary: {"total_equity", "available_equity", "hl_details", "bp_details"},
}


def log_trading_event(
    logger: structlog.BoundLogger,
    event_type: str,
    model: BaseModel,
    exclude_sensitive: bool = True,
    **extra_context: Any,
) -> None:
    """Log a trading event using existing models.

    Args:
        logger: Structlog logger instance
        event_type: Type of event (e.g., "order_placed", "position_opened")
        model: Pydantic model instance to log
        exclude_sensitive: Whether to exclude sensitive fields
        **extra_context: Additional context to include in the log
    """
    exclude_fields = set()
    if exclude_sensitive:
        exclude_fields = SENSITIVE_FIELDS.get(type(model), set())

    logger.info(
        event_type,
        **model.model_dump(mode="json", exclude=exclude_fields),
        **extra_context,
    )


def log_order_lifecycle(
    logger: structlog.BoundLogger,
    order: Order,
    event: str,
    **context: Any,
) -> None:
    """Log order lifecycle events with consistent structure.

    Args:
        logger: Structlog logger instance
        order: Order instance
        event: Lifecycle event (placed, filled, cancelled, etc.)
        **context: Additional context
    """
    log_trading_event(
        logger,
        f"order_{event}",
        order,
        order_lifecycle_event=event,
        **context,
    )


def log_position_update(
    logger: structlog.BoundLogger,
    position: DerivativePosition,
    action: str,
    **context: Any,
) -> None:
    """Log position updates with consistent structure.

    Args:
        logger: Structlog logger instance
        position: Position instance
        action: Position action (opened, closed, updated, liquidated)
        **context: Additional context
    """
    log_trading_event(
        logger,
        f"position_{action}",
        position,
        position_action=action,
        **context,
    )
```

Usage in modules:

```python
# In any module using structured logging
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.logging.logging_helpers import log_trading_event, log_order_lifecycle

logger = get_logger(__name__)

# Direct usage with any model
order = Order(...)  # Existing order instance
log_trading_event(logger, "order_placed", order, strategy="funding_arb")

# Or use specialized helpers
log_order_lifecycle(logger, order, "filled", fill_price=actual_price)

# For performance-sensitive paths, log less frequently
if should_log:  # e.g., every Nth event or on significant changes
    log_trading_event(logger, "tick_processed", ticker)
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
- [x] Implement `structlog_config.py`
- [x] Create `cyberdelta/logging/logging_helpers.py`
- [x] Update `main.py` to use new configuration
- [x] Test file logging works correctly
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
- ✅ **PRESERVE OR ENHANCE message informativeness during migration**
- ✅ Include a `message` field for complex scenarios that need human-readable context
- ✅ Ensure event names are self-documenting (e.g., `order_fill_triggering_trade_processing` not just `order_filled`)

### Don'ts
- ❌ Don't log sensitive data (API keys, secrets)
- ❌ Don't use print statements
- ❌ Don't mix logging styles in same module
- ❌ Don't over-log in hot paths
- ❌ Don't forget to bind correlation IDs
- ❌ **Don't lose information when converting from f-strings to structured logs**
- ❌ Don't use generic event names that don't convey the action
- ❌ Don't assume structured fields alone are always sufficient for complex scenarios

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
