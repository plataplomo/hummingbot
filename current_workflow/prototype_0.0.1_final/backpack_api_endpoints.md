# Backpack API Endpoints Documentation

## API Base URLs

- **REST API Base**: `https://api.backpack.exchange`
- **WebSocket API Base**: `wss://ws.backpack.exchange`

## Authentication

Signed requests require the following headers:

- `X-Timestamp` - Unix time in milliseconds that the request was sent
- `X-Window` - Time window in milliseconds that the request is valid for (default: `5000`, maximum: `60000`)
- `X-API-Key` - Base64 encoded verifying key of the ED25519 keypair
- `X-Signature` - Base64 encoded signature generated according to the documentation

The signing string follows the format:
```
instruction=<instruction_type>&param1=value1&param2=value2&timestamp=<timestamp>&window=<window>
```

### Instruction Types

```
accountQuery
balanceQuery
borrowLendExecute
borrowHistoryQueryAll
borrowPositionHistoryQueryAll
collateralQuery
depositAddressQuery
depositQueryAll
fillHistoryQueryAll
fundingHistoryQueryAll
interestHistoryQueryAll
orderCancel
orderCancelAll
orderExecute
orderHistoryQueryAll
orderQuery
orderQueryAll
pnlHistoryQueryAll
positionQuery
quoteSubmit
subscribe (for WebSocket)
withdraw
withdrawalQueryAll
```

## Market Data Endpoints

| Endpoint | Description | Parameters | Authentication |
|----------|-------------|------------|----------------|
| `/api/v1/assets` | Get information on all assets | None | No |
| `/api/v1/markets` | Get market information | None | No |
| `/api/v1/market` | Get a specific market | `symbol` | No |
| `/api/v1/ticker` | Get 24hr price change statistics | `symbol` (optional) | No |
| `/api/v1/ticker/price` | Get latest price for symbol(s) | `symbol` (optional) | No |
| `/api/v1/ticker/bookTicker` | Get best price/qty on the order book | `symbol` (optional) | No |
| `/api/v1/klines` | Get kline/candlestick data | `symbol`, `interval`, `startTime` (optional), `endTime` (optional), `limit` (optional) | No |
| `/api/v1/depth` | Get order book depth | `symbol`, `limit` (optional) | No |
| `/api/v1/trades` | Get recent trades | `symbol`, `limit` (optional) | No |
| `/api/v1/openInterest` | Get open interest | `symbol` (optional) | No |

## Funding Rate Endpoints

| Endpoint | Description | Parameters | Authentication |
|----------|-------------|------------|----------------|
| `/api/v1/markPrices` | Get mark prices with funding info | `symbol` (optional) | No |
| `/api/v1/fundingRates` | Get funding interval rate history | `symbol`, `limit` (optional), `offset` (optional) | No |

## Trading Endpoints

| Endpoint | Description | Parameters | Instruction Type | Authentication |
|----------|-------------|------------|------------------|----------------|
| `/api/v1/order` | Get open order | `symbol`, `orderId` or `clientId` | `orderQuery` | Yes |
| `/api/v1/order` | Place an order | Request body | `orderExecute` | Yes |
| `/api/v1/order` | Cancel an order | `symbol`, `orderId` or `clientId` | `orderCancel` | Yes |
| `/api/v1/orders` | Get all open orders | `symbol` (optional), `marketType` (optional) | `orderQueryAll` | Yes |
| `/api/v1/orders/cancel` | Cancel all orders | `symbol` (optional) | `orderCancelAll` | Yes |
| `/api/v1/history/orders` | Get order history | `symbol` (optional), `status` (optional), `orderId` (optional), `limit` (optional), `offset` (optional), `from` (optional), `to` (optional) | `orderHistoryQueryAll` | Yes |
| `/api/v1/history/fills` | Get fill history | `symbol` (optional), `orderId` (optional), `limit` (optional), `offset` (optional), `from` (optional), `to` (optional) | `fillHistoryQueryAll` | Yes |
| `/api/v1/position` | Get position information | `symbol` (optional) | `positionQuery` | Yes |

## Account Endpoints

| Endpoint | Description | Parameters | Instruction Type | Authentication |
|----------|-------------|------------|------------------|----------------|
| `/api/v1/capital` | Get account balances | None | `balanceQuery` | Yes |
| `/api/v1/capital/deposits` | Get deposit history | `symbol` (optional), `status` (optional), `limit` (optional), `offset` (optional), `from` (optional), `to` (optional) | `depositQueryAll` | Yes |
| `/api/v1/capital/address` | Get deposit address | `blockchain`, `symbol` | `depositAddressQuery` | Yes |
| `/api/v1/capital/withdrawals` | Get withdrawal history | `symbol` (optional), `status` (optional), `limit` (optional), `offset` (optional), `from` (optional), `to` (optional) | `withdrawalQueryAll` | Yes |
| `/api/v1/capital/withdraw` | Request withdrawal | Request body | `withdraw` | Yes |

## WebSocket API

### Connection

Connect to `wss://ws.backpack.exchange` to access WebSocket streams.

### Subscribing

To subscribe to streams, send:
```json
{
  "method": "SUBSCRIBE",
  "params": ["stream1", "stream2", ...],
  "signature": ["<verifying key>", "<signature>", "<timestamp>", "<window>"] // Only for private streams
}
```

For private streams, the signature instruction type is `subscribe`.

### Stream Naming Format

- Market tickers: `ticker.<symbol>`
- Depth (orderbook): `depth.<symbol>` 
- Aggregated depth: `depth.200ms.<symbol>` or `depth.1000ms.<symbol>`
- Klines (candles): `kline.<interval>.<symbol>`
- Trades: `trade.<symbol>`
- Mark prices: `markPrice.<symbol>`
- Book ticker: `bookTicker.<symbol>`
- Open interest: `openInterest.<symbol>`

### Private Streams

- Account orders: `account.orderUpdate` or `account.orderUpdate.<symbol>`
- Account positions: `account.positionUpdate` or `account.positionUpdate.<symbol>`
- RFQ updates: `account.rfqUpdate` or `account.rfqUpdate.<symbol>`

### Timestamp Precision

- Event (`E`) and engine (`T`) timestamps in WebSocket events are in **microseconds**
- REST API timestamps are in **milliseconds**

### Keeping the Connection Alive

- Server sends ping every 60 seconds
- Client must respond with a pong within 120 seconds
- If a pong is not received, the connection will be closed

## Important Notes

- Symbol format is typically `BTC_USDC`, `ETH_USDT`, etc.
- Order ID format has changed and is no longer a byte-shifted timestamp
- For order updates, the `O` field indicates the origin (USER, LIQUIDATION_AUTOCLOSE, etc.)
- The depth endpoint includes a `timestamp` field with the system time in microseconds
- WebSocket streaming now supports multiple subscriptions in a single request
- Position queries will include details like liquidation price, entry price, and PnL
- For futures markets, funding rate information is available via the markPrices endpoint
- Historical funding rates can be queried with pagination via the fundingRates endpoint

## Common Error Responses

The API returns a JSON error response with:

```json
{
  "code": "ERROR_CODE",
  "message": "Error description"
}
```

Common error codes include:
- `UNAUTHORIZED`: Invalid API credentials
- `BAD_REQUEST`: Invalid parameters
- `INTERNAL_ERROR`: Internal server error
- `INSUFFICIENT_FUNDS`: Not enough balance for the operation
- `INVALID_PRICE`: Price does not conform to tick size
- `INVALID_QUANTITY`: Quantity does not conform to step size
- `PRICE_BAND`: Price is outside the allowed range 