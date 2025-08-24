# Spot Connector v2.1 Developer Checklist

## Prerequisites
Before setting up the Spot Connector, you must have the Hummingbot source version installed on your system. Detailed installation instructions can be found in the Hummingbot Installation Guide.

## File Structure

### Main Connector Files
```
hummingbot/hummingbot/connector/exchange/connector_name/
  __init__.py
  connector_name_api_order_book_data_source.py
  connector_name_api_user_stream_data_source.py
  connector_name_auth.py
  connector_name_constants.py
  connector_name_exchange.py
  connector_name_order_book.py
  connector_name_utils.py
  connector_name_web_utils.py
  dummy.pxd
  dummy.pyx
```

### Test Files
```
hummingbot/test/hummingbot/exchange/connector_name/
  __init__.py
  test_connector_name_api_order_book_data_source.py
  test_connector_name_api_user_stream_data_source.py
  test_connector_name_auth.py
  test_connector_name_exchange.py
  test_connector_name_order_book.py
  test_connector_name_utils.py
  test_connector_name_web_utils.py
```

## API Requirements Checklist

### Public REST Endpoints (Required)
- **GET ACTIVE MARKETS**: Endpoint to get the list of active trading pairs
  - Example: https://binance-docs.github.io/apidocs/spot/en/#exchange-information
- **GET LAST TRADED PRICE**: Endpoint for latest price
- **GET ORDERBOOK SNAPSHOT**: Endpoint for order book data
- **PING ENDPOINT**: Endpoint to test connectivity
  - Example: https://binance-docs.github.io/apidocs/spot/en/#test-connectivity
- **SERVER TIME**: Public REST endpoint for server time

### Trading Rules (Required)
- **[TR] MINIMUM NOTIONAL SIZE**: Endpoint to get trading rules
- **[TR] MINIMUM ORDER SIZE**: Endpoint to get trading rules
- **[TR] MINIMUM PRICE**: Endpoint to get trading rules
- **[TR] ORDER INCREMENT**: Endpoint to get trading rules
  - Example: https://binance-docs.github.io/apidocs/spot/en/#exchange-information

### Private REST Endpoints (Required)
- **GET ACCOUNT BALANCE**: Endpoint to get the current account balance
  - Example: https://binance-docs.github.io/apidocs/spot/en/#account-information-user_data
- **GET ORDER STATUS BY client_order_id**: Check order by client ID
- **GET ORDER STATUS BY exchange_order_id**: Check order by exchange ID
- **GET TRADES HISTORY BY ORDER ID**: Get trade history for an order
- **GET TRADES HISTORY BY TIMESTAMPS**: Get trades within time range
- **GET OPEN ORDERS**: Endpoint to get active orders
  - Example: https://binance-docs.github.io/apidocs/spot/en/#query-order-user_data
- **CREATE ORDERS**: Endpoint to create new orders
  - Example: https://binance-docs.github.io/apidocs/spot/en/#new-order-trade
- **CREATE ORDERS with client_order_id**: Create orders with custom ID
- **CANCEL ORDER BY exchange_order_id**: Cancel by exchange ID
- **CANCEL ORDER BY client_order_id**: Cancel by client ID

### Public WebSocket Channels (Required)
- **ORDERBOOK CHANNEL (Min depth 100)**: Public orders channel
  - Example: https://binance-docs.github.io/apidocs/spot/en/#diff-depth-stream
- **ORDERBOOK DIFF CHANNEL**: Order book updates channel
- **PUBLIC TRADES CHANNEL**: Public trades stream

### Private WebSocket Channels (Required)
- **USER ORDER/TRADES CHANNEL**: Private orders updates channel
  - Example: https://binance-docs.github.io/apidocs/spot/en/#payload-order-update
- **USER BALANCE CHANNEL**: Private balance events channel (total and available balance)
  - Example: https://binance-docs.github.io/apidocs/spot/en/#payload-account-update

### Other Requirements
- **AUTHENTICATION**: Class for configuring authenticated requests
  - Example: https://github.com/hummingbot/hummingbot/blob/master/hummingbot/connector/exchange/binance/binance_auth.py
- **Rate limits**: Documentation of rate limits for endpoints and global limits
  - Example: https://binance-docs.github.io/apidocs/spot/en/#limits

## Implementation Steps

### 1. Constants Configuration
```python
# Add to connector_name_constants.py
DEFAULT_DOMAIN = "your_default_domain"
REST_URLS = {"your_domain": "your_rest_url"} # or f-string
RATE_LIMITS = [...] # Define rate limits
SERVER_TIME_PATH_URL = "/api/v1/time" # Example path for server time sync
```

### 2. Web Utils Configuration
```python
# In connector_name_web_utils.py
# Copy Binance web_utils and replace 'Binance' with 'ConnectorName' (uppercase)
# and 'binance' with 'connector_name'.

# Function to create REST URL
def build_rest_url(path_url):
    # Logic to construct the full URL using DEFAULT_DOMAIN and REST_URLS
    pass

# If Time Sync is needed:
# Implement build_api_factory_without_time_synchronizer_pre_processor
# Implement get_current_server_time
# Code build_api_factory with time synchronizer

# If Time Sync is not needed:
# Delete time sync files and code build_api_factory without it
```

### 3. Utils Configuration
```python
# In connector_name_utils.py
# Copy Binance utils and replace 'Binance' with 'ConnectorName' (uppercase)
# and 'binance' with 'connector_name'.

DEFAULT_FEES = {"your_fee_structure": "values"}

# Check and adjust domain connections if necessary
```

### 4. Order Book Implementation
```python
# In connector_name_order_book.py
# Copy Binance order book file and replace 'Binance' with 'ConnectorName' (uppercase)
# and 'binance' with 'connector_name'.

class ConnectorNameOrderBook:
    # Initial structure for import purposes
    pass
```

### 5. Order Book Data Source
```python
# connector_name_api_order_book_data_source.py

# Copy the Binance connector order book data source file.
# Replace Binance for `connector_name` with the first letter in uppercase.
# Replace binance for `connector_name`.
# Replace the `HEARTBEAT_TIME_INTERVAL` with the appropriate value.
```

### 6. Authentication Implementation
```python
# connector_name_auth.py

# Copy the Binance connector auth file.
# Replace Binance for `connector_name` with the first letter in uppercase.
```

### 7. User Stream Data Source
```python
# Copy the Binance connector user stream data source file.
# Replace Binance for `connector_name` with the first letter in uppercase.
# Replace binance for `connector_name`.
# Replace the `HEARTBEAT_TIME_INTERVAL` with the appropriate value.
# Check if you need `LISTEN_KEY_KEEP_ALIVE_INTERVAL`.
```

### 8. Exchange Connector File
```python
# connector_name_exchange.py
# Copy the Binance connector exchange file.
# Replace Binance for `connector_name` with the first letter in uppercase.
# Replace binance for `connector_name`.
```

## ExchangePyBase Methods to Implement

These methods must be implemented from the `ExchangePyBase` class:

1. `authenticator` - Authentication handler
2. `name` - Connector name
3. `rate_limits_rules` - Rate limiting configuration
4. `domain` - Exchange domain
5. `client_order_id_max_length` - Maximum client order ID length
6. `client_order_id_prefix` - Prefix for client order IDs
7. `trading_rules_request_path` - API path for trading rules
8. `trading_pairs_request_path` - API path for trading pairs
9. `check_network_request_path` - API path for network check
10. `trading_pairs` - List of trading pairs
11. `is_cancel_request_in_exchange_synchronous` - Cancel order behavior
12. `is_trading_required` - Trading requirement flag
13. `supported_order_types` - List of supported order types
14. `_is_request_exception_related_to_time_synchronizer` - Time sync error detection
15. `_create_web_assistants_factory` - Web assistants factory creation
16. `_create_order_book_data_source` - Order book data source creation
17. `_create_user_stream_data_source` - User stream data source creation
18. `_get_fee` - Fee calculation
19. `_place_order` - Order placement logic
20. `_place_cancel` - Order cancellation logic
21. `_format_trading_rules` - Trading rules formatting
22. `_status_polling_loop_fetch_updates` - Status polling updates
23. `_update_trading_fees` - Trading fees update
24. `_user_stream_event_listener` - User stream event handler
25. `_all_trade_updates_for_order` - Trade updates for order
26. `_request_order_status` - Request order status
27. `_update_balances` - Balance update logic
28. `_get_last_traded_price` - Get last traded price
29. `_initialize_trading_pair_symbols_from_exchange_info` - Initialize trading pairs

## Generic Test Class Methods

These methods need to be implemented for the generic test class:

- `all_symbols_request_mock_response`
- `latest_prices_request_mock_response`
- `all_symbols_including_invalid_pair_mock_response`
- `network_status_request_successful_mock_response`
- `trading_rules_request_mock_response`
- `trading_rules_request_erroneous_mock_response`
- `order_creation_request_successful_mock_response`
- `balance_request_mock_response_for_base_and_quote`
- `balance_request_mock_response_only_base`
- `balance_event_websocket_update`
- `expected_latest_price`
- `expected_supported_order_types`
- `expected_trading_rule`
- `expected_logged_error_for_erroneous_trading_rule`
- `expected_exchange_order_id`
- `is_cancel_request_executed_synchronously_by_server`
- `is_order_fill_http_update_included_in_status_update`
- `is_order_fill_http_update_executed_during_websocket_order_event_processing`
- `expected_partial_fill_price`
- `expected_partial_fill_amount`
- `expected_fill_fee`
- `expected_fill_trade_id`
- `exchange_symbol_for_tokens`
- `create_exchange_instance`
- `validate_auth_credentials_present`
- `validate_order_creation_request`
- `validate_order_cancelation_request`
- `validate_order_status_request`
- `validate_trades_request`
- `configure_successful_cancelation_response`
- `configure_erroneous_cancelation_response`
- `configure_one_successful_one_erroneous_cancel_all_response`
- `configure_completely_filled_order_status_response`
- `configure_canceled_order_status_response`
- `configure_erroneous_http_fill_trade_response`
- `configure_open_order_status_response`
- `configure_http_error_order_status_response`
- `configure_partially_filled_order_status_response`
- `configure_partial_fill_trade_response`
- `configure_full_fill_trade_response`
- `order_event_for_new_order_websocket_update`
- `order_event_for_canceled_order_websocket_update`
- `order_event_for_full_fill_websocket_update`
- `trade_event_for_full_fill_websocket_update`

## Testing Implementation

### Order Book Data Source Tests
Use TDD approach for implementing these methods:

#### REST Tests
- `test_get_new_order_book_successful`
- `_order_book_snapshot`
- `_request_order_book_snapshot`
- `ExchangeOrderBook.snapshot_message_from_exchange`
- `test_get_new_order_book_raises_exception`

#### WebSocket Tests
- `test_listen_for_subscriptions_subscribes_to_trades_and_order_diffs`
- `listen_for_subscriptions`
- `_subscribe_channels`
- `_process_websocket_messages`
- `test_listen_for_subscriptions_raises_cancel_exception`
- `test_listen_for_subscriptions_logs_exception_details`
- `test_subscribe_channels_raises_cancel_exception`
- `test_subscribe_channels_raises_exception_and_logs_error`

### User Stream Tests
- `test_listen_for_user_stream_get_listen_key_successful_with_user_update_event`
- `listen_for_user_stream` (is in the superclass)
- `_connected_websocket_assistant`
- `_subscribe_channels`
- `_get_ws_assistant`
- `_on_user_stream_interruption`
- `test_listen_for_user_stream_does_not_queue_empty_payload`
- `test_listen_for_user_stream_connection_failed`
- `test_listen_for_user_stream_iter_message_throws_exception`

## Global Configuration

Add to `conf_global_TEMPLATE.yml`:

```yaml
connector_name_api_key: "" # Replace with your API key for the connector
connector_name_api_secret: "" # Replace with your API secret for the connector
```

## Additional Implementation Details

### Order Fill Updates
Implement `update_order_fills` method for tracking executed orders.

### Time Synchronizer (if needed)
- `test_update_time_synchronizer_successfully`
- `_update_time_synchronizer`
- `test_time_synchronizer_related_request_error_detection`

## Contribution Requirements

When submitting a new connector:

1. **Connector folder**: Complete set of connector files based on the templates
2. **Adherence to standard**: Connector should pass both Developer and QA Checklist
3. **Unit tests**: Pull request should pass code coverage checks
4. **Documentation**: Accompanying documentation pull request to `hummingbot-site` repo
5. **Inline code comments**: Particularly for code materially different from templates

## Submission Process

1. Fork the [Hummingbot](https://github.com/hummingbot/hummingbot) repository
2. Add a `spot` connector that fulfills the requirements above
3. Submit a pull request to the `development` branch in Github
4. Submit a [New Connector Proposal](https://snapshot.org/#/hbot-ncp.eth) in the Hummingbot NCP Snapshot

## Resources

- [Spot Connector v2.1 Notion Template](https://hummingbot-foundation.notion.site/Spot-Connector-v2-1-1cc43830938445c9974f43ef861d59f1)
- [Spot Connector QA Checklist](/developers/connectors/test/)
- [Debugging Guide](/developers/connectors/debug/)
