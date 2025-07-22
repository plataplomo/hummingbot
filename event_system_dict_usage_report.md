# Event System Dict[str, Any] Usage Report

## Summary
After a comprehensive search of the event system in `cyberdelta/core/portfolio/events/`, I found several instances where `dict[str, Any]` is still being used and could be replaced with proper Pydantic models.

## Issues Found

### 1. `_serialize_data()` Method Returns
- **Location**: All event classes
- **Current**: Returns `dict[str, Any]`
- **Note**: This appears to be intentional for serialization purposes and is likely acceptable as it's an internal method for converting typed data to dictionary format.

### 2. Balance Events (`balance_events.py`)

#### BalanceReconciledEvent
- **Line 160**: `discrepancies: dict[str, dict[str, Any]] | None = None`
- **Issue**: Using nested dicts for discrepancies
- **Solution**: Should use `dict[str, ReconciliationDiscrepancy]` from the reconciliation service

#### BalanceErrorData
- **Line 217**: `error_data: dict[str, Any] = Field(default_factory=dict)`
- **Issue**: Untyped error data
- **Solution**: Could use `ErrorContext` from `portfolio_types/domain_models.py`

### 3. Position Events (`position_events.py`)

#### PositionErrorData
- **Line 287**: `error_data: dict[str, Any] = Field(default_factory=dict)`
- **Issue**: Untyped error data
- **Solution**: Could use `ErrorContext` from `portfolio_types/domain_models.py`

### 4. Trade Events (`trade_events.py`)

#### TradeValidatedEvent.create()
- **Line 74**: `validation_results: dict[str, Any] | None = None`
- **Issue**: Untyped validation results
- **Solution**: Could use `TradeValidationResult` from `screening/trade_data_screener.py`

#### RejectedTradeData
- **Line 188**: `raw_data: dict[str, Any] = Field(default_factory=dict)`
- **Issue**: Storing raw trade data as untyped dict
- **Solution**: This might be acceptable as it's meant to store arbitrary raw data from exchanges

#### TradeRejectedEvent.create()
- **Line 198**: `trade_data: dict[str, Any]`
- **Issue**: Accepting untyped trade data
- **Solution**: Could accept a partial Trade object or create a RawTradeData model

### 5. Error Events (`error_events.py`)

#### ErrorData
- **Line 28**: `context: dict[str, str | int | float | bool] | None = None`
- **Issue**: Using dict for context instead of ErrorContext model
- **Solution**: Use `ErrorContext` from `portfolio_types/domain_models.py`

#### ComponentStateData  
- **Line 70**: `metadata: dict[str, str | int | float | bool] | None = None`
- **Issue**: Untyped metadata
- **Solution**: Create a proper ComponentMetadata model

#### StateSnapshotData
- **Line 93**: `state_data: dict[str, Any]`
- **Issue**: Flexible state data storage
- **Note**: This might be intentional for flexibility with different state types

### 6. Event Dispatcher (`base/event_dispatcher.py`)

#### Metrics Storage
- **Line 83**: `self._metrics: dict[str, Any] = {`
- **Issue**: Untyped metrics storage
- **Solution**: Could create a proper EventMetrics model

#### Handler Info
- **Line 379**: `def get_handler_info(self) -> list[dict[str, Any]]:`
- **Issue**: Returns untyped handler information
- **Solution**: Create a HandlerInfo model

## Recommendations

1. **High Priority**: Replace error_data fields with ErrorContext model
2. **Medium Priority**: Replace validation_results with proper validation models
3. **Medium Priority**: Create models for discrepancies using existing ReconciliationDiscrepancy
4. **Low Priority**: Keep _serialize_data() as dict[str, Any] (intentional for serialization)
5. **Low Priority**: Keep raw_data fields as dict[str, Any] (intentional for arbitrary data)

## Existing Models Available for Use

- `ErrorContext` - in `portfolio_types/domain_models.py`
- `ReconciliationDiscrepancy` - in `services/reconciliation/portfolio_reconciliation_service.py`
- `TradeValidationResult` - in `screening/trade_data_screener.py`
- `StateValidationMetadata` - in `portfolio_types/state_types.py`