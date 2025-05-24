# WebSocket Testing Guide for Concrete API Implementations

## Overview

This guide provides comprehensive testing strategies for WebSocket functionality in concrete API implementations (`BackpackAPI`, `HyperliquidAPI`). It covers testing the abstract methods that were made concrete in the base `ExchangeAPI` class and the new abstract methods that must be implemented by concrete classes.

## Testing Strategy Summary

### 1. Abstract Methods to Test in Concrete Implementations

The following methods are now **abstract** and must be tested in concrete API implementations:

- `_construct_subscription_payload(topic: str) -> dict[str, Any] | None`
- `_handle_websocket_message(message: dict[str, Any]) -> None`
- `_route_ws_message(message: dict[str, Any]) -> None`

### 2. Concrete Methods to Test Integration With

The following methods are now **concrete** in the base class and should be tested for proper integration:

- `subscribe(topic: str, handler: MessageHandler) -> None`
- `_resubscribe() -> None`
- `_on_ws_connected() -> None`

## Detailed Testing Requirements

### A. Testing `_construct_subscription_payload`

**Purpose**: Ensure each exchange produces correct subscription messages for their WebSocket API.

**Test Cases**:

```python
class TestBackpackAPIWebSocketSubscriptionPayloads:
    """Test Backpack-specific subscription payload construction."""
    
    def test_construct_subscription_payload_for_ticker(self, bp_api_with_di):
        """Test ticker subscription payload construction."""
        api = bp_api_with_di()
        
        # Test ticker subscription
        payload = api._construct_subscription_payload("ticker.BTC-USD")
        
        expected = {
            "method": "SUBSCRIBE",
            "params": ["ticker.BTC-USD"],
            "id": 1  # or whatever Backpack expects
        }
        assert payload == expected
    
    def test_construct_subscription_payload_for_orderbook(self, bp_api_with_di):
        """Test orderbook subscription payload construction."""
        api = bp_api_with_di()
        
        payload = api._construct_subscription_payload("orderbook.BTC-USD")
        
        expected = {
            "method": "SUBSCRIBE", 
            "params": ["orderbook.BTC-USD"],
            "id": 2
        }
        assert payload == expected
    
    def test_construct_subscription_payload_for_trades(self, bp_api_with_di):
        """Test trades subscription payload construction."""
        api = bp_api_with_di()
        
        payload = api._construct_subscription_payload("trades.BTC-USD")
        
        expected = {
            "method": "SUBSCRIBE",
            "params": ["trades.BTC-USD"], 
            "id": 3
        }
        assert payload == expected
    
    def test_construct_subscription_payload_invalid_topic(self, bp_api_with_di):
        """Test handling of invalid/unsupported topics."""
        api = bp_api_with_di()
        
        # Should return None for unsupported topics
        payload = api._construct_subscription_payload("invalid.topic")
        assert payload is None
    
    def test_construct_subscription_payload_edge_cases(self, bp_api_with_di):
        """Test edge cases like empty strings, special characters."""
        api = bp_api_with_di()
        
        # Empty topic
        assert api._construct_subscription_payload("") is None
        
        # Topic with special characters
        payload = api._construct_subscription_payload("ticker.BTC-USD@100ms")
        # Should handle or reject appropriately based on exchange specs
```

### B. Testing `_handle_websocket_message`

**Purpose**: Ensure proper parsing, validation, and routing of incoming WebSocket messages.

**Test Cases**:

```python
class TestBackpackAPIWebSocketMessageHandling:
    """Test Backpack-specific WebSocket message handling."""
    
    @pytest.mark.asyncio
    async def test_handle_websocket_message_ticker_update(self, bp_api_with_di):
        """Test handling of ticker update messages."""
        mock_ws_raw_handler = MagicMock()
        mock_data_mapper = MagicMock()
        
        api = bp_api_with_di()
        # Inject mocks for the message handling pipeline
        api._ticker_ws_raw_handler = mock_ws_raw_handler
        api._ticker_data_mapper = mock_data_mapper
        
        # Mock the transformation pipeline
        mock_raw_ticker = MagicMock()
        mock_internal_ticker = MagicMock()
        mock_ws_raw_handler.validate_and_parse.return_value = mock_raw_ticker
        mock_data_mapper.transform_raw_to_internal.return_value = mock_internal_ticker
        
        # Register a handler for ticker updates
        received_data = []
        async def ticker_handler(data, full_message):
            received_data.append((data, full_message))
        
        await api.subscribe("ticker.BTC-USD", ticker_handler)
        
        # Simulate incoming ticker message
        ticker_message = {
            "stream": "ticker.BTC-USD",
            "data": {
                "symbol": "BTC-USD",
                "price": "50000.00",
                "volume": "1000.50"
            }
        }
        
        await api._handle_websocket_message(ticker_message)
        
        # Verify the message processing pipeline
        mock_ws_raw_handler.validate_and_parse.assert_called_once_with(ticker_message["data"])
        mock_data_mapper.transform_raw_to_internal.assert_called_once_with(mock_raw_ticker)
        
        # Verify the handler was called with transformed data
        assert len(received_data) == 1
        assert received_data[0][0] == mock_internal_ticker
        assert received_data[0][1] == ticker_message
    
    @pytest.mark.asyncio
    async def test_handle_websocket_message_orderbook_update(self, bp_api_with_di):
        """Test handling of orderbook update messages."""
        # Similar structure to ticker test but for orderbook data
        pass
    
    @pytest.mark.asyncio
    async def test_handle_websocket_message_trade_update(self, bp_api_with_di):
        """Test handling of trade update messages."""
        # Similar structure for trade data
        pass
    
    @pytest.mark.asyncio
    async def test_handle_websocket_message_error_response(self, bp_api_with_di):
        """Test handling of error responses from WebSocket."""
        api = bp_api_with_di()
        
        error_message = {
            "error": {
                "code": 1001,
                "message": "Invalid subscription"
            }
        }
        
        # Should log error and not crash
        with pytest.raises(APIError):
            await api._handle_websocket_message(error_message)
    
    @pytest.mark.asyncio
    async def test_handle_websocket_message_unknown_stream(self, bp_api_with_di):
        """Test handling of messages for unknown/unsubscribed streams."""
        api = bp_api_with_di()
        
        unknown_message = {
            "stream": "unknown.stream",
            "data": {"some": "data"}
        }
        
        # Should handle gracefully (log warning, don't crash)
        await api._handle_websocket_message(unknown_message)
    
    @pytest.mark.asyncio
    async def test_handle_websocket_message_malformed_data(self, bp_api_with_di):
        """Test handling of malformed/invalid message data."""
        api = bp_api_with_di()
        
        # Missing required fields
        malformed_message = {
            "stream": "ticker.BTC-USD"
            # Missing "data" field
        }
        
        # Should handle validation errors gracefully
        with pytest.raises(APIError):
            await api._handle_websocket_message(malformed_message)
    
    @pytest.mark.asyncio
    async def test_handle_websocket_message_transformation_failure(self, bp_api_with_di):
        """Test handling when data transformation fails."""
        mock_ws_raw_handler = MagicMock()
        mock_data_mapper = MagicMock()
        
        api = bp_api_with_di()
        api._ticker_ws_raw_handler = mock_ws_raw_handler
        api._ticker_data_mapper = mock_data_mapper
        
        # Mock transformation failure
        mock_ws_raw_handler.validate_and_parse.side_effect = ValueError("Invalid data")
        
        ticker_message = {
            "stream": "ticker.BTC-USD",
            "data": {"invalid": "data"}
        }
        
        # Should handle transformation errors gracefully
        with pytest.raises(APIError):
            await api._handle_websocket_message(ticker_message)
```

### C. Testing `_route_ws_message`

**Purpose**: Ensure correct identification and routing of different message types to appropriate handlers.

**Test Cases**:

```python
class TestBackpackAPIWebSocketMessageRouting:
    """Test Backpack-specific WebSocket message routing."""
    
    @pytest.mark.asyncio
    async def test_route_ws_message_ticker_identification(self, bp_api_with_di):
        """Test correct identification and routing of ticker messages."""
        api = bp_api_with_di()
        
        # Mock the specific message handler
        api._handle_ticker_message = AsyncMock()
        
        ticker_message = {
            "stream": "ticker.BTC-USD",
            "data": {"symbol": "BTC-USD", "price": "50000"}
        }
        
        await api._route_ws_message(ticker_message)
        
        # Verify correct handler was called
        api._handle_ticker_message.assert_called_once_with(ticker_message)
    
    @pytest.mark.asyncio
    async def test_route_ws_message_orderbook_identification(self, bp_api_with_di):
        """Test correct identification and routing of orderbook messages."""
        api = bp_api_with_di()
        
        api._handle_orderbook_message = AsyncMock()
        
        orderbook_message = {
            "stream": "orderbook.BTC-USD",
            "data": {"bids": [], "asks": []}
        }
        
        await api._route_ws_message(orderbook_message)
        
        api._handle_orderbook_message.assert_called_once_with(orderbook_message)
    
    @pytest.mark.asyncio
    async def test_route_ws_message_trade_identification(self, bp_api_with_di):
        """Test correct identification and routing of trade messages."""
        api = bp_api_with_di()
        
        api._handle_trade_message = AsyncMock()
        
        trade_message = {
            "stream": "trades.BTC-USD", 
            "data": {"price": "50000", "quantity": "1.0"}
        }
        
        await api._route_ws_message(trade_message)
        
        api._handle_trade_message.assert_called_once_with(trade_message)
    
    @pytest.mark.asyncio
    async def test_route_ws_message_user_data_identification(self, bp_api_with_di):
        """Test correct identification and routing of user-specific messages."""
        api = bp_api_with_di()
        
        api._handle_user_data_message = AsyncMock()
        
        user_message = {
            "stream": "user.orders",
            "data": {"orderId": "123", "status": "filled"}
        }
        
        await api._route_ws_message(user_message)
        
        api._handle_user_data_message.assert_called_once_with(user_message)
    
    @pytest.mark.asyncio
    async def test_route_ws_message_unknown_type(self, bp_api_with_di):
        """Test handling of unknown message types."""
        api = bp_api_with_di()
        
        unknown_message = {
            "stream": "unknown.type",
            "data": {"some": "data"}
        }
        
        # Should handle gracefully (log warning, don't crash)
        await api._route_ws_message(unknown_message)
```

### D. Integration Testing with Base Class Methods

**Purpose**: Ensure concrete implementations work correctly with the base class WebSocket functionality.

**Test Cases**:

```python
class TestBackpackAPIWebSocketIntegration:
    """Test integration between concrete implementation and base class."""
    
    @pytest.mark.asyncio
    async def test_subscribe_integration_with_construct_payload(self, bp_api_with_di):
        """Test that subscribe correctly uses _construct_subscription_payload."""
        mock_ws = MagicMock()
        mock_ws.send_json = AsyncMock(return_value=True)
        mock_ws.is_connected = True
        mock_ws.close = AsyncMock()
        
        api = bp_api_with_di(ws_manager=mock_ws)
        
        async def test_handler(data, full_message):
            pass
        
        # Subscribe to a topic
        await api.subscribe("ticker.BTC-USD", test_handler)
        
        # Verify the correct Backpack-specific payload was sent
        expected_payload = {
            "method": "SUBSCRIBE",
            "params": ["ticker.BTC-USD"],
            "id": 1
        }
        mock_ws.send_json.assert_called_once_with(expected_payload)
    
    @pytest.mark.asyncio
    async def test_resubscription_after_reconnection(self, bp_api_with_di):
        """Test that resubscription works with Backpack-specific payloads."""
        mock_ws = MagicMock()
        mock_ws.send_json = AsyncMock(return_value=True)
        mock_ws.is_connected = True
        mock_ws.close = AsyncMock()
        
        api = bp_api_with_di(ws_manager=mock_ws)
        
        async def handler1(data, full_message):
            pass
        async def handler2(data, full_message):
            pass
        
        # Subscribe to multiple topics
        await api.subscribe("ticker.BTC-USD", handler1)
        await api.subscribe("orderbook.ETH-USD", handler2)
        
        # Clear mock to test resubscription
        mock_ws.send_json.reset_mock()
        
        # Simulate reconnection by calling the connection callback
        if hasattr(api, '_on_ws_connected'):
            await getattr(api, '_on_ws_connected')()
        
        # Verify both subscriptions were re-sent with correct payloads
        assert mock_ws.send_json.call_count == 2
        
        calls = mock_ws.send_json.call_args_list
        payloads = [call[0][0] for call in calls]
        
        expected_payloads = [
            {"method": "SUBSCRIBE", "params": ["ticker.BTC-USD"], "id": 1},
            {"method": "SUBSCRIBE", "params": ["orderbook.ETH-USD"], "id": 2}
        ]
        
        # Sort both lists to handle potential ordering differences
        payloads.sort(key=lambda x: x["params"][0])
        expected_payloads.sort(key=lambda x: x["params"][0])
        
        assert payloads == expected_payloads
    
    @pytest.mark.asyncio
    async def test_end_to_end_message_flow(self, bp_api_with_di):
        """Test complete message flow from subscription to handler invocation."""
        mock_ws = MagicMock()
        mock_ws.send_json = AsyncMock(return_value=True)
        mock_ws.is_connected = True
        mock_ws.close = AsyncMock()
        
        # Mock the transformation pipeline
        mock_ticker_handler = MagicMock()
        mock_ticker_mapper = MagicMock()
        
        api = bp_api_with_di(ws_manager=mock_ws)
        api._ticker_ws_raw_handler = mock_ticker_handler
        api._ticker_data_mapper = mock_ticker_mapper
        
        # Set up transformation mocks
        mock_raw_ticker = MagicMock()
        mock_internal_ticker = MagicMock()
        mock_ticker_handler.validate_and_parse.return_value = mock_raw_ticker
        mock_ticker_mapper.transform_raw_to_internal.return_value = mock_internal_ticker
        
        # Register handler and subscribe
        received_data = []
        async def user_handler(data, full_message):
            received_data.append((data, full_message))
        
        await api.subscribe("ticker.BTC-USD", user_handler)
        
        # Simulate incoming message
        incoming_message = {
            "stream": "ticker.BTC-USD",
            "data": {
                "symbol": "BTC-USD",
                "price": "50000.00",
                "volume": "1000.50"
            }
        }
        
        # Process the message through the complete pipeline
        await api._handle_websocket_message(incoming_message)
        
        # Verify the complete flow
        mock_ticker_handler.validate_and_parse.assert_called_once()
        mock_ticker_mapper.transform_raw_to_internal.assert_called_once_with(mock_raw_ticker)
        
        assert len(received_data) == 1
        assert received_data[0][0] == mock_internal_ticker
        assert received_data[0][1] == incoming_message
```

## Exchange-Specific Considerations

### Backpack API WebSocket Testing

**Key Areas**:
- Subscription message format: `{"method": "SUBSCRIBE", "params": [...], "id": N}`
- Message identification: Usually by `stream` field
- Error handling: Check for `error` field in responses
- Authentication: May require signed subscription messages for private streams

### Hyperliquid API WebSocket Testing

**Key Areas**:
- Subscription message format: `{"method": "subscribe", "subscription": {...}}`
- Message identification: Usually by `channel` or `type` field
- User data streams: Require wallet address for authentication
- Rate limiting: May have subscription rate limits

## Error Handling Test Patterns

### Common Error Scenarios to Test

1. **Invalid Subscription Payloads**:
   ```python
   def test_invalid_topic_handling(self, api_with_di):
       # Test that invalid topics return None from _construct_subscription_payload
       assert api._construct_subscription_payload("invalid.format") is None
   ```

2. **Malformed Incoming Messages**:
   ```python
   @pytest.mark.asyncio
   async def test_malformed_message_handling(self, api_with_di):
       # Test that malformed messages don't crash the handler
       with pytest.raises(APIError):
           await api._handle_websocket_message({"incomplete": "message"})
   ```

3. **Transformation Failures**:
   ```python
   @pytest.mark.asyncio
   async def test_transformation_error_handling(self, api_with_di):
       # Mock transformation failure and verify graceful handling
       api._ticker_mapper.transform_raw_to_internal.side_effect = ValueError("Bad data")
       # ... test that error is handled appropriately
   ```

## Performance and Concurrency Testing

### Message Processing Performance

```python
@pytest.mark.asyncio
async def test_high_frequency_message_processing(self, api_with_di):
    """Test handling of high-frequency message streams."""
    api = api_with_di()
    
    # Simulate rapid message processing
    messages = [create_test_message(i) for i in range(1000)]
    
    start_time = time.time()
    for message in messages:
        await api._handle_websocket_message(message)
    end_time = time.time()
    
    # Verify performance is acceptable
    processing_time = end_time - start_time
    assert processing_time < 1.0  # Should process 1000 messages in under 1 second
```

### Concurrent Handler Execution

```python
@pytest.mark.asyncio
async def test_concurrent_handler_execution(self, api_with_di):
    """Test that multiple handlers can process messages concurrently."""
    api = api_with_di()
    
    handler_calls = []
    
    async def slow_handler(data, full_message):
        await asyncio.sleep(0.1)  # Simulate slow processing
        handler_calls.append("slow")
    
    async def fast_handler(data, full_message):
        handler_calls.append("fast")
    
    # Subscribe both handlers to different topics
    await api.subscribe("ticker.BTC-USD", slow_handler)
    await api.subscribe("ticker.ETH-USD", fast_handler)
    
    # Send messages to both topics simultaneously
    await asyncio.gather(
        api._handle_websocket_message(create_btc_message()),
        api._handle_websocket_message(create_eth_message())
    )
    
    # Verify both handlers were called
    assert "slow" in handler_calls
    assert "fast" in handler_calls
```

## Summary

This testing guide ensures comprehensive coverage of WebSocket functionality in concrete API implementations. The key principles are:

1. **Test Exchange-Specific Logic**: Focus on the concrete implementations of abstract methods
2. **Test Integration**: Ensure concrete methods work with base class functionality  
3. **Test Error Handling**: Cover all failure modes gracefully
4. **Test Performance**: Ensure acceptable performance under load
5. **Test Real-World Scenarios**: Include end-to-end message flows

By following this guide, you'll have robust test coverage for WebSocket functionality that catches issues early and ensures reliable operation in production. 