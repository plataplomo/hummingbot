# WebSocket Developer Guide

## Getting Started

This guide covers how to work with the CyberDeltaEngine WebSocket architecture, including adding new exchanges, message types, and implementing custom processing logic.

## Table of Contents

1. [Adding a New Exchange](#adding-a-new-exchange)
2. [Adding New Message Types](#adding-new-message-types)
3. [Testing Guidelines](#testing-guidelines)
4. [Troubleshooting](#troubleshooting)
5. [Best Practices](#best-practices)

## Adding a New Exchange

### Step 1: Create Exchange Models

Create Pydantic models for the exchange's WebSocket messages:

```python
# cyberdelta/apis/newexchange/models/ws_models.py
from cyberdelta.apis.base.ws_models import BaseWebSocketMessage
from pydantic import Field
from typing import Literal

class NewExchangeDepthUpdate(BaseWebSocketMessage):
    """Order book depth update from NewExchange."""
    
    channel: Literal["depth"]
    symbol: str
    bids: list[list[float]] = Field(description="List of [price, quantity] pairs")
    asks: list[list[float]] = Field(description="List of [price, quantity] pairs")
    timestamp: int = Field(description="Exchange timestamp")
    sequence: int = Field(description="Sequence number")

class NewExchangeTradeUpdate(BaseWebSocketMessage):
    """Trade update from NewExchange."""
    
    channel: Literal["trades"]
    symbol: str
    side: Literal["buy", "sell"]
    price: float
    quantity: float
    timestamp: int
    trade_id: str
```

### Step 2: Create Domain Models

Create internal domain models that your application will work with:

```python
# cyberdelta/apis/newexchange/models/domain_models.py
from cyberdelta.core.models.market_data import BaseDepthUpdate, BaseTrade
from pydantic import Field

class NewExchangeDepthDomain(BaseDepthUpdate):
    """Domain model for NewExchange depth updates."""
    
    exchange: str = "newexchange"
    sequence_number: int
    server_timestamp: int

class NewExchangeTradeDomain(BaseTrade):
    """Domain model for NewExchange trades."""
    
    exchange: str = "newexchange"
    trade_id: str
    server_timestamp: int
```

### Step 3: Create Transformers

Implement transformers to convert from raw WebSocket models to domain models:

```python
# cyberdelta/apis/newexchange/transformers.py
from cyberdelta.apis.newexchange.models.ws_models import (
    NewExchangeDepthUpdate, NewExchangeTradeUpdate
)
from cyberdelta.apis.newexchange.models.domain_models import (
    NewExchangeDepthDomain, NewExchangeTradeDomain
)

class NewExchangeDepthTransformer:
    """Transform NewExchange depth updates to domain models."""
    
    def transform(self, validated: NewExchangeDepthUpdate) -> NewExchangeDepthDomain:
        """Transform validated depth update to domain model."""
        return NewExchangeDepthDomain(
            symbol=validated.symbol,
            bids=validated.bids,
            asks=validated.asks,
            sequence_number=validated.sequence,
            server_timestamp=validated.timestamp,
            timestamp=validated.timestamp  # Use exchange timestamp
        )

class NewExchangeTradeTransformer:
    """Transform NewExchange trade updates to domain models."""
    
    def transform(self, validated: NewExchangeTradeUpdate) -> NewExchangeTradeDomain:
        """Transform validated trade to domain model."""
        return NewExchangeTradeDomain(
            symbol=validated.symbol,
            side=validated.side,
            price=validated.price,
            quantity=validated.quantity,
            trade_id=validated.trade_id,
            server_timestamp=validated.timestamp,
            timestamp=validated.timestamp
        )
```

### Step 4: Create WebSocket Router

Implement the exchange-specific WebSocket router:

```python
# cyberdelta/apis/newexchange/ws_router.py
from cyberdelta.apis.base.ws_router import BaseWebSocketRouter
from cyberdelta.apis.base.ws_processor import PydanticWebSocketProcessor
from cyberdelta.apis.base.ws_error_handler import BaseErrorHandler
from cyberdelta.apis.newexchange.models.ws_models import (
    NewExchangeDepthUpdate, NewExchangeTradeUpdate
)
from cyberdelta.apis.newexchange.transformers import (
    NewExchangeDepthTransformer, NewExchangeTradeTransformer
)

class NewExchangeWebSocketRouter(BaseWebSocketRouter):
    """WebSocket router for NewExchange."""
    
    def __init__(self, error_handler: BaseErrorHandler) -> None:
        """Initialize NewExchange router."""
        super().__init__(
            exchange_name="newexchange",
            error_handler=error_handler
        )
    
    def _setup_processors(self) -> None:
        """Setup message processors for NewExchange."""
        # Depth processor
        depth_processor = PydanticWebSocketProcessor(
            raw_model=NewExchangeDepthUpdate,
            transformer=NewExchangeDepthTransformer(),
            error_handler=self.error_handler,
            processor_name="NewExchangeDepth"
        )
        self.processors["depth"] = depth_processor
        
        # Trade processor
        trade_processor = PydanticWebSocketProcessor(
            raw_model=NewExchangeTradeUpdate,
            transformer=NewExchangeTradeTransformer(),
            error_handler=self.error_handler,
            processor_name="NewExchangeTrade"
        )
        self.processors["trades"] = trade_processor
    
    def _extract_routing_key(self, message: dict[str, Any]) -> str | None:
        """Extract routing key from NewExchange message."""
        # NewExchange uses 'channel' field for routing
        return message.get("channel")
    
    def _extract_payload(self, message: dict[str, Any]) -> Any:
        """Extract payload from NewExchange message."""
        # For NewExchange, the entire message is the payload
        return message
```

### Step 5: Create Integration Tests

Write comprehensive tests for your new exchange integration:

```python
# tests/integration/apis/newexchange/test_ws_integration.py
import pytest
from cyberdelta.apis.newexchange.ws_router import NewExchangeWebSocketRouter
from cyberdelta.apis.base.ws_error_handler import BaseErrorHandler

class TestNewExchangeIntegration:
    """Integration tests for NewExchange WebSocket handling."""
    
    @pytest.fixture
    def router(self):
        """Create router for testing."""
        error_handler = BaseErrorHandler()
        return NewExchangeWebSocketRouter(error_handler)
    
    @pytest.mark.asyncio
    async def test_depth_update_processing(self, router):
        """Test depth update message processing."""
        # Mock message handler
        received_data = []
        async def depth_handler(domain_model: dict, original_message: dict):
            received_data.append(domain_model)
        
        handlers = {"depth": depth_handler}
        
        # Sample depth message
        message = {
            "channel": "depth",
            "symbol": "BTCUSD",
            "bids": [[50000.0, 1.5], [49999.0, 2.0]],
            "asks": [[50001.0, 1.0], [50002.0, 1.5]],
            "timestamp": 1640995200000,
            "sequence": 12345
        }
        
        # Process message
        await router.route_message(message, handlers)
        
        # Verify processing
        assert len(received_data) == 1
        domain_data = received_data[0]
        assert domain_data["symbol"] == "BTCUSD"
        assert domain_data["exchange"] == "newexchange"
        assert len(domain_data["bids"]) == 2
        assert len(domain_data["asks"]) == 2
```

### Step 6: Update API Client

Integrate the router into your API client:

```python
# cyberdelta/apis/newexchange/api.py
from cyberdelta.apis.newexchange.ws_router import NewExchangeWebSocketRouter
from cyberdelta.apis.base.validated_ws_manager import ValidatedWebSocketManager

class NewExchangeAPI:
    """API client for NewExchange."""
    
    def __init__(self):
        """Initialize NewExchange API."""
        self.error_handler = BaseErrorHandler()
        self.ws_router = NewExchangeWebSocketRouter(self.error_handler)
        self.ws_manager = ValidatedWebSocketManager()
    
    async def start_websocket(self, handlers: dict):
        """Start WebSocket connection with message handlers."""
        async def message_handler(message: dict):
            await self.ws_router.route_message(message, handlers)
        
        await self.ws_manager.connect(
            uri="wss://api.newexchange.com/ws",
            message_handler=message_handler
        )
```

## Adding New Message Types

### Step 1: Define the Raw Message Model

Add a new Pydantic model for the raw WebSocket message:

```python
class NewExchangeOrderUpdate(BaseWebSocketMessage):
    """Order status update from NewExchange."""
    
    channel: Literal["orders"]
    order_id: str
    symbol: str
    side: Literal["buy", "sell"]
    order_type: Literal["market", "limit"]
    status: Literal["open", "filled", "cancelled"]
    filled_quantity: float
    remaining_quantity: float
    average_price: float | None = None
    timestamp: int
```

### Step 2: Create Domain Model

```python
class NewExchangeOrderDomain(BaseOrder):
    """Domain model for NewExchange orders."""
    
    exchange: str = "newexchange"
    order_id: str
    server_timestamp: int
    filled_amount: float
    remaining_amount: float
```

### Step 3: Implement Transformer

```python
class NewExchangeOrderTransformer:
    """Transform order updates to domain models."""
    
    def transform(self, validated: NewExchangeOrderUpdate) -> NewExchangeOrderDomain:
        """Transform validated order update."""
        return NewExchangeOrderDomain(
            order_id=validated.order_id,
            symbol=validated.symbol,
            side=validated.side,
            order_type=validated.order_type,
            status=validated.status,
            filled_amount=validated.filled_quantity,
            remaining_amount=validated.remaining_quantity,
            average_price=validated.average_price,
            server_timestamp=validated.timestamp,
            timestamp=validated.timestamp
        )
```

### Step 4: Register Processor

Add the processor to your router's `_setup_processors` method:

```python
def _setup_processors(self) -> None:
    """Setup message processors."""
    # ... existing processors ...
    
    # Order processor
    order_processor = PydanticWebSocketProcessor(
        raw_model=NewExchangeOrderUpdate,
        transformer=NewExchangeOrderTransformer(),
        error_handler=self.error_handler,
        processor_name="NewExchangeOrder"
    )
    self.processors["orders"] = order_processor
```

### Step 5: Add Handler

Create a message handler in your application:

```python
async def handle_order_update(domain_model: dict, original_message: dict):
    """Handle order status updates."""
    order_data = NewExchangeOrderDomain.model_validate(domain_model)
    
    # Update internal order state
    await order_manager.update_order(order_data)
    
    # Trigger any necessary actions
    if order_data.status == "filled":
        await portfolio_manager.update_positions(order_data)
```

## Testing Guidelines

### Unit Tests

**Test Categories:**
1. **Model Validation**: Test Pydantic models with valid/invalid data
2. **Transformers**: Test data transformation logic
3. **Processors**: Test processing pipeline with mocked dependencies
4. **Routers**: Test message routing and error handling

**Example Unit Test:**

```python
class TestNewExchangeDepthTransformer:
    """Test NewExchange depth transformer."""
    
    def test_transform_valid_depth(self):
        """Test transformation of valid depth update."""
        raw_depth = NewExchangeDepthUpdate(
            channel="depth",
            symbol="BTCUSD",
            bids=[[50000.0, 1.5]],
            asks=[[50001.0, 1.0]],
            timestamp=1640995200000,
            sequence=12345
        )
        
        transformer = NewExchangeDepthTransformer()
        domain_model = transformer.transform(raw_depth)
        
        assert domain_model.exchange == "newexchange"
        assert domain_model.symbol == "BTCUSD"
        assert len(domain_model.bids) == 1
        assert domain_model.sequence_number == 12345
```

### Integration Tests

**Test Scenarios:**
1. **End-to-End Message Flow**: Raw message → domain model → handler
2. **Error Handling**: Invalid messages, transformation errors
3. **Performance**: Message throughput and latency
4. **Reconnection**: Connection loss and recovery

**Example Integration Test:**

```python
@pytest.mark.asyncio
async def test_message_flow_with_metrics(router):
    """Test complete message flow with metrics collection."""
    metrics_collector = WebSocketMetricsCollector("newexchange")
    router.metrics_collector = metrics_collector
    
    processed_messages = []
    async def test_handler(domain_model: dict, original: dict):
        processed_messages.append(domain_model)
    
    handlers = {"depth": test_handler}
    
    # Process multiple messages
    for i in range(100):
        message = create_test_depth_message(sequence=i)
        await router.route_message(message, handlers)
    
    # Verify metrics
    summary = metrics_collector.get_summary()
    assert summary["message_count"].count == 100
    assert summary["processing_time"].count == 100
    assert len(processed_messages) == 100
```

### Performance Tests

**Metrics to Test:**
- Message processing throughput (messages/second)
- Processing latency (milliseconds)
- Memory usage under load
- Error rates under stress

**Example Performance Test:**

```python
@pytest.mark.performance
async def test_high_throughput_processing():
    """Test processing under high message load."""
    router = create_test_router()
    message_count = 10000
    start_time = time.time()
    
    handlers = {"depth": lambda d, o: None}  # No-op handler
    
    tasks = []
    for i in range(message_count):
        message = create_test_message()
        task = router.route_message(message, handlers)
        tasks.append(task)
    
    await asyncio.gather(*tasks)
    
    duration = time.time() - start_time
    throughput = message_count / duration
    
    assert throughput > 1000  # At least 1000 msg/sec
    assert duration < 20  # Complete in under 20 seconds
```

## Troubleshooting

### Common Issues

#### 1. ValidationError: Model Validation Failed

**Symptoms:**
- Messages are rejected with Pydantic validation errors
- Error logs show "ValidationError" with field details

**Diagnosis:**
```python
# Enable detailed validation logging
import logging
logging.getLogger("cyberdelta.apis.base.ws_processor").setLevel(logging.DEBUG)
```

**Solutions:**
- Check message format against exchange documentation
- Verify field types and constraints in Pydantic models
- Add field validators for complex validation logic
- Handle optional fields with `Field(default=None)`

#### 2. UnroutableMessage: No Processor Found

**Symptoms:**
- Messages are received but not processed
- Error logs show "no processor found for routing key"

**Diagnosis:**
- Check router's `_extract_routing_key()` implementation
- Verify processor registration in `_setup_processors()`
- Ensure routing key matches registered processors

**Solutions:**
```python
# Debug routing key extraction
def _extract_routing_key(self, message: dict) -> str | None:
    key = message.get("channel")  # or "topic", "type", etc.
    self.logger.debug("extracted_routing_key", key=key, message_keys=list(message.keys()))
    return key
```

#### 3. TransformationError: Model Transformation Failed

**Symptoms:**
- Validation succeeds but transformation fails
- Error logs show transformation exceptions

**Solutions:**
- Check transformer logic for edge cases
- Handle missing or null fields gracefully
- Add error logging in transformers
- Validate transformer output with unit tests

#### 4. High Processing Latency

**Symptoms:**
- Messages process slowly
- High processing time in metrics

**Diagnosis:**
```python
# Enable performance logging
processor.logger.info("processing_performance", 
                     validation_time=validation_time,
                     transformation_time=transform_time,
                     handler_time=handler_time)
```

**Solutions:**
- Profile handler performance
- Optimize database queries in handlers
- Use async/await properly
- Consider message batching for bulk operations

### Debugging Tools

#### 1. Enable Debug Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Specific component logging
logging.getLogger("cyberdelta.apis.base.ws_router").setLevel(logging.DEBUG)
logging.getLogger("cyberdelta.apis.base.ws_processor").setLevel(logging.DEBUG)
```

#### 2. Use Metrics for Monitoring

```python
# Check processing metrics
metrics = processor.get_metrics()
print(f"Total processed: {metrics['metrics']['total_processed']}")
print(f"Error rate: {metrics['metrics']['error_rate']}")
print(f"Avg processing time: {metrics['metrics']['average_processing_time_ms']}ms")
```

#### 3. Mock WebSocket Messages for Testing

```python
def create_test_message(symbol="BTCUSD", **overrides):
    """Create test WebSocket message."""
    message = {
        "channel": "depth",
        "symbol": symbol,
        "bids": [[50000.0, 1.0]],
        "asks": [[50001.0, 1.0]],
        "timestamp": int(time.time() * 1000),
        "sequence": 1
    }
    message.update(overrides)
    return message
```

## Best Practices

### 1. Model Design

- **Use Strict Types**: Prefer `Literal` types over generic strings
- **Validate Constraints**: Add field validators for business rules
- **Handle Optionals**: Use `Field(default=None)` for optional fields
- **Document Fields**: Add clear descriptions to all fields

```python
class WellDesignedModel(BaseWebSocketMessage):
    """Well-designed WebSocket message model."""
    
    channel: Literal["depth", "trades", "orders"] = Field(
        description="Message channel identifier"
    )
    symbol: str = Field(
        min_length=1, max_length=20,
        description="Trading pair symbol (e.g., BTCUSD)"
    )
    timestamp: int = Field(
        gt=0, description="Unix timestamp in milliseconds"
    )
    
    @field_validator('symbol')
    @classmethod
    def validate_symbol_format(cls, v):
        """Validate symbol format."""
        if not v.isupper():
            raise ValueError("Symbol must be uppercase")
        return v
```

### 2. Error Handling

- **Graceful Degradation**: Handle errors without crashing
- **Meaningful Logs**: Provide context in error messages
- **Error Recovery**: Implement retry logic for transient errors
- **Circuit Breakers**: Prevent cascade failures

```python
async def robust_message_handler(domain_model: dict, original: dict):
    """Robust message handler with error handling."""
    try:
        # Process message
        result = await process_market_data(domain_model)
        
    except ValidationError as e:
        logger.warning("invalid_domain_model", error=str(e), model=domain_model)
        # Continue processing other messages
        
    except DatabaseError as e:
        logger.error("database_error", error=str(e))
        # Retry with exponential backoff
        await retry_with_backoff(process_market_data, domain_model)
        
    except Exception as e:
        logger.exception("unexpected_error", error=str(e))
        # Alert monitoring system
        await send_alert("WebSocket processing error", str(e))
```

### 3. Performance Optimization

- **Minimize Allocations**: Reuse objects where possible
- **Batch Operations**: Group database operations
- **Async Properly**: Use async/await consistently
- **Profile Regularly**: Monitor performance metrics

```python
class OptimizedHandler:
    """Performance-optimized message handler."""
    
    def __init__(self):
        self.batch_size = 100
        self.batch_buffer = []
        self.last_flush = time.time()
    
    async def handle_message(self, domain_model: dict, original: dict):
        """Handle message with batching."""
        self.batch_buffer.append(domain_model)
        
        # Flush batch when full or after timeout
        if (len(self.batch_buffer) >= self.batch_size or 
            time.time() - self.last_flush > 1.0):
            await self.flush_batch()
    
    async def flush_batch(self):
        """Flush batched messages to database."""
        if not self.batch_buffer:
            return
            
        try:
            await database.bulk_insert(self.batch_buffer)
            self.batch_buffer.clear()
            self.last_flush = time.time()
        except Exception as e:
            logger.error("batch_flush_error", error=str(e))
```

### 4. Testing Strategy

- **Unit Test Models**: Test all validation scenarios
- **Mock External Dependencies**: Use mocks for WebSocket connections
- **Test Error Paths**: Ensure error handling works correctly
- **Performance Testing**: Validate throughput requirements

```python
class ComprehensiveTestSuite:
    """Comprehensive testing approach."""
    
    def test_model_validation_success(self):
        """Test successful model validation."""
        # Test with valid data
        
    def test_model_validation_errors(self):
        """Test all validation error scenarios."""
        # Test with invalid data
        
    def test_transformer_edge_cases(self):
        """Test transformer with edge cases."""
        # Test with edge case data
        
    @pytest.mark.asyncio
    async def test_end_to_end_flow(self):
        """Test complete message flow."""
        # Test from WebSocket message to handler
        
    @pytest.mark.performance
    async def test_throughput_requirements(self):
        """Test performance requirements."""
        # Test with high message volume
```

This developer guide provides a comprehensive foundation for working with the WebSocket architecture. Follow these patterns and best practices to ensure reliable, maintainable, and performant implementations.